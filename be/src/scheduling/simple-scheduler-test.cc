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

#include <gtest/gtest.h>

#include <set>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "simple-scheduler.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(pool_conf_file);

namespace impala {

// Typedefs to make the rest of the code more readable.
typedef string Hostname;
typedef string IpAddr;
typedef string TableName;

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
  for (int idx: idxs) out->push_back(in[idx]);
}

/// Helper classes to be used by the scheduler tests.

/// Overall testing approach: Each test builds a list of hosts and a plan, both to which
/// elements can be added using various helper methods. Then scheduling can be tested
/// by instantiating SchedulerWrapper and calling Compute(...). The result can be verified
/// using a set of helper methods. There are also helper methods to modify the internal
/// state of the scheduler between subsequent calls to SchedulerWrapper::Compute().
///
/// The model currently comes with some known limitations:
///
/// - Files map 1:1 to blocks and to scan ranges.
/// - All files have the same size (1 block of 1M). Tables that differ in size can be
///   expressed as having a different number of blocks.
/// - We don't support multiple backends on a single host.
/// - Ports are assigned to hosts automatically and are not configurable by the test.

// TODO: Extend the model to support files with multiple blocks.
// TODO: Test more methods of the scheduler.
// TODO: Add support to add skewed table scans with multiple scan ranges: often there are
//       3 replicas where there may be skew for 1 of the replicas (e.g. after a single
//       node insert) but the other 2 are random.
// TODO: Make use of the metrics provided by the scheduler.
// TODO: Add checks for MinNumAssignmentsPerHost() to all tests where applicable.
// TODO: Add post-condition checks that have to hold for all successful scheduler runs.
// TODO: Add possibility to explicitly specify the replica location per file.
// TODO: Add methods to retrieve and verify generated file placements from plan.
// TODO: Extend the model to specify a physical schema independently of a plan (ie,
//       tables/files, blocks, replicas and cached replicas exist independently of the
//       queries that run against them).

/// File blocks store a list of all datanodes that have a replica of the block. When
/// defining tables you can specify the desired replica placement among all available
/// datanodes in the cluster.
///
/// - RANDOM means that any datanode can be picked.
/// - LOCAL_ONLY means that only datanodes with a backend will be picked.
/// - REMOTE_ONLY means that only datanodes without a backend will be picked.
///
/// Whether replicas will be cached or not is not determined by this value, but by
/// additional function arguments when adding tables to the schema.
enum class ReplicaPlacement {
  RANDOM,
  LOCAL_ONLY,
  REMOTE_ONLY,
};

/// Host model. Each host can have either a backend, a datanode, or both. To specify that
/// a host should not act as a backend or datanode specify '-1' as the respective port.
struct Host {
  Host(const Hostname& name, const IpAddr& ip, int be_port, int dn_port)
      : name(name), ip(ip), be_port(be_port), dn_port(dn_port) {}
  Hostname name;
  IpAddr ip;
  int be_port;  // Backend port
  int dn_port;  // Datanode port
};

/// A cluster stores a list of hosts and provides various methods to add hosts to the
/// cluster. All hosts are guaranteed to have unique IP addresses and hostnames.
class Cluster {
 public:
  /// Add a host and return the host's index. 'hostname' and 'ip' of the new host will be
  /// generated and are guaranteed to be unique.
  int AddHost(bool has_backend, bool has_datanode) {
    int host_idx = hosts_.size();
    int be_port = has_backend ? BACKEND_PORT : -1;
    int dn_port = has_datanode ? DATANODE_PORT : -1;
    IpAddr ip = HostIdxToIpAddr(host_idx);
    DCHECK(ip_to_idx_.find(ip) == ip_to_idx_.end());
    ip_to_idx_[ip] = host_idx;
    hosts_.push_back(Host(HostIdxToHostname(host_idx), ip, be_port, dn_port));
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

  /// Add a number of hosts with the same properties by repeatedly calling AddHost(..).
  void AddHosts(int num_hosts, bool has_backend, bool has_datanode) {
    for (int i = 0; i < num_hosts; ++i) AddHost(true, true);
  }

  /// Convert a host index to a hostname.
  static Hostname HostIdxToHostname(int host_idx) {
    return HOSTNAME_PREFIX + std::to_string(host_idx);
  }

  /// Return the backend address (ip, port) for the host with index 'host_idx'.
  void GetBackendAddress(int host_idx, TNetworkAddress* addr) const {
    DCHECK_LT(host_idx, hosts_.size());
    addr->hostname = hosts_[host_idx].ip;
    addr->port = hosts_[host_idx].be_port;
  }

  const vector<Host>& hosts() const { return hosts_; }
  int NumHosts() const { return hosts_.size(); }

  /// These methods return lists of host indexes, grouped by their type, which can be used
  /// to draw samples of random sets of hosts.
  /// TODO: Think of a nicer abstraction to expose this information.
  const vector<int>& backend_host_idxs() const { return backend_host_idxs_; }
  const vector<int>& datanode_host_idxs() const { return datanode_host_idxs_; }

  const vector<int>& datanode_with_backend_host_idxs() const {
    return datanode_with_backend_host_idxs_;
  }

  const vector<int>& datanode_only_host_idxs() const { return datanode_only_host_idxs_; }

 private:
  /// Port for all backends.
  static const int BACKEND_PORT;

  /// Port for all datanodes.
  static const int DATANODE_PORT;

  /// Prefix for all generated hostnames.
  static const string HOSTNAME_PREFIX;

  /// First octet for all generated IP addresses.
  static const string IP_PREFIX;

  /// List of hosts in this cluster.
  vector<Host> hosts_;

  /// Lists of indexes of hosts, grouped by their type. The lists reference hosts in
  /// 'hosts_' by index and are used for random sampling.
  ///
  /// All hosts with a backend.
  vector<int> backend_host_idxs_;
  /// All hosts with a datanode.
  vector<int> datanode_host_idxs_;
  /// All hosts with a datanode and a backend.
  vector<int> datanode_with_backend_host_idxs_;
  /// All hosts with a datanode but no backend.
  vector<int> datanode_only_host_idxs_;

  /// Map from IP addresses to host indexes.
  unordered_map<IpAddr, int> ip_to_idx_;

  /// Convert a host index to an IP address. The host index must be smaller than 2^24 and
  /// will specify the lower 24 bits of the IPv4 address (the lower 3 octets).
  static IpAddr HostIdxToIpAddr(int host_idx) {
    DCHECK_LT(host_idx, (1 << 24));
    string suffix;
    for (int i = 0; i < 3; ++i) {
      suffix = "." + std::to_string(host_idx % 256) + suffix;  // prepend
      host_idx /= 256;
    }
    DCHECK_EQ(0, host_idx);
    return IP_PREFIX + suffix;
  }
};

const int Cluster::BACKEND_PORT = 1000;
const int Cluster::DATANODE_PORT = 2000;
const string Cluster::HOSTNAME_PREFIX = "host_";
const string Cluster::IP_PREFIX = "10";

struct Block {
  /// By default all blocks are of the same size.
  int64_t length = DEFAULT_BLOCK_SIZE;

  /// Index into the cluster that owns the table that owns this block.
  vector<int> replica_host_idxs;

  /// Flag for each entry in replica_host_idxs whether it is a cached replica or not.
  vector<bool> replica_host_idx_is_cached;

  /// Default size for new blocks.
  static const int64_t DEFAULT_BLOCK_SIZE;
};
/// Default size for new blocks is 1MB.
const int64_t Block::DEFAULT_BLOCK_SIZE = 1 << 20;

struct Table {
  vector<Block> blocks;
};

class Schema {
 public:
  Schema(const Cluster& cluster) : cluster_(cluster) {}

  /// Add a table consisting of a single block to the schema with explicitly specified
  /// replica indexes for non-cached replicas and without any cached replicas. Replica
  /// indexes must refer to hosts in cluster_.hosts() by index.
  void AddSingleBlockTable(const TableName& table_name,
      const vector<int>& non_cached_replica_host_idxs) {
    AddSingleBlockTable(table_name, non_cached_replica_host_idxs, {});
  }

  /// Add a table consisting of a single block to the schema with explicitly specified
  /// replica indexes for both non-cached and cached replicas. Values in both lists must
  /// refer to hosts in cluster_.hosts() by index. Both lists must be disjoint, i.e., a
  /// replica can either be cached or not.
  void AddSingleBlockTable(const TableName& table_name,
      const vector<int>& non_cached_replica_host_idxs,
      const vector<int>& cached_replica_host_idxs) {
    DCHECK(tables_.find(table_name) == tables_.end());
    Block block;
    int num_replicas = non_cached_replica_host_idxs.size() +
      cached_replica_host_idxs.size();
    block.replica_host_idxs = non_cached_replica_host_idxs;
    block.replica_host_idxs.insert(block.replica_host_idxs.end(),
        cached_replica_host_idxs.begin(), cached_replica_host_idxs.end());
    // Initialize for non-cached replicas first.
    block.replica_host_idx_is_cached.resize(non_cached_replica_host_idxs.size(), false);
    // Fill up to final size for cached replicas.
    block.replica_host_idx_is_cached.insert(block.replica_host_idx_is_cached.end(),
        cached_replica_host_idxs.size(), true);
    DCHECK_EQ(block.replica_host_idxs.size(), block.replica_host_idx_is_cached.size());
    DCHECK_EQ(block.replica_host_idxs.size(), num_replicas);
    // Create table
    Table table;
    table.blocks.push_back(block);
    // Insert table
    tables_.emplace(table_name, table);
  }

  /// Add a table to the schema, selecting replica hosts according to the given replica
  /// placement preference. All replicas will be non-cached.
  void AddMultiBlockTable(const TableName& table_name, int num_blocks,
      ReplicaPlacement replica_placement, int num_replicas) {
    AddMultiBlockTable(table_name, num_blocks, replica_placement, num_replicas, 0);
  }

  /// Add a table to the schema, selecting replica hosts according to the given replica
  /// placement preference. After replica selection has been done, 'num_cached_replicas'
  /// of them are marked as cached.
  void AddMultiBlockTable(const TableName& table_name, int num_blocks,
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
          SampleNElements(num_replicas, cluster_.datanode_with_backend_host_idxs(),
              &replica_idxs);
          break;
        case ReplicaPlacement::REMOTE_ONLY:
          DCHECK(num_replicas <= cluster_.datanode_only_host_idxs().size());
          SampleNElements(num_replicas, cluster_.datanode_only_host_idxs(),
              &replica_idxs);
          break;
        default:
          DCHECK(false) << "Unsupported replica placement: "
              << (int)replica_placement;
      }

      // Determine cached replicas.
      vector<int> cached_replicas;
      vector<bool>& is_cached = block.replica_host_idx_is_cached;
      is_cached.resize(num_replicas, false);
      SampleN(num_cached_replicas, num_replicas, &cached_replicas);
      // Flag cached entries.
      for (const int idx: cached_replicas) is_cached[idx] = true;

      DCHECK_EQ(replica_idxs.size(), is_cached.size());
      table.blocks.push_back(block);
    }
    // Insert table
    tables_[table_name] = table;
  }

  const Table& GetTable(const TableName& table_name) const {
    auto it = tables_.find(table_name);
    DCHECK(it != tables_.end());
    return it->second;
  }

  const Cluster& cluster() const { return cluster_; }

 private:
  /// Store a reference to the cluster, from which hosts are sampled. Test results will
  /// use the cluster to resolve host indexes to hostnames and IP addresses.
  const Cluster& cluster_;

  unordered_map<TableName, Table> tables_;
};

/// Plan model. A plan contains a list of tables to scan and the query options to be used
/// during scheduling.
class Plan {
 public:
  Plan(const Schema& schema) : schema_(schema) {}

  const TQueryOptions& query_options() const { return query_options_; }

  void SetRandomReplica(bool b) { query_options_.schedule_random_replica = b; }
  void SetDisableCachedReads(bool b) { query_options_.disable_cached_reads = b; }
  const Cluster& cluster() const { return schema_.cluster(); }

  const vector<TNetworkAddress>& referenced_datanodes() const {
    return referenced_datanodes_;
  }

  const vector<TScanRangeLocations>& scan_range_locations() const {
    return scan_range_locations_;
  }

  /// Add a scan of table 'table_name' to the plan. This method will populate the internal
  /// list of TScanRangeLocations and can be called multiple times for the same table to
  /// schedule additional scans.
  void AddTableScan(const TableName& table_name) {
    const Table& table = schema_.GetTable(table_name);
    const vector<Block>& blocks = table.blocks;
    for (int i = 0; i < blocks.size(); ++i) {
      const Block& block = blocks[i];
      TScanRangeLocations scan_range_locations;
      BuildTScanRangeLocations(table_name, block, i, &scan_range_locations);
      scan_range_locations_.push_back(scan_range_locations);
    }
  }

 private:
  /// Store a reference to the schema, from which scanned tables will be read.
  const Schema& schema_;

  TQueryOptions query_options_;

  /// List of all datanodes that are referenced by this plan. Only hosts that have an
  /// assigned scan range are added here.
  vector<TNetworkAddress> referenced_datanodes_;

  /// Map from plan host index to an index in 'referenced_datanodes_'.
  boost::unordered_map<int, int> host_idx_to_datanode_idx_;

  /// List of all scan range locations, which can be passed to the SimpleScheduler.
  vector<TScanRangeLocations> scan_range_locations_;

  /// Initialize a TScanRangeLocations object in place.
  void BuildTScanRangeLocations(const TableName& table_name, const Block& block,
      int block_idx, TScanRangeLocations* scan_range_locations) {
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

  void BuildScanRange(const TableName& table_name, const Block& block, int block_idx,
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

  /// Look up the plan-local host index of 'cluster_datanode_idx'. If the host has not
  /// been added to the plan before, it will add it to 'referenced_datanodes_' and return
  /// the new index.
  int FindOrInsertDatanodeIndex(int cluster_datanode_idx) {
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
};

class Result {
 private:
  /// Map to count the number of assignments per backend.
  typedef unordered_map<IpAddr, int> NumAssignmentsPerBackend;

  /// Map to count the number of assigned bytes per backend.
  typedef unordered_map<IpAddr, int64_t> NumAssignedBytesPerBackend;

  /// Parameter type for callbacks, which are used to filter scheduling results.
  struct AssignmentInfo {
    const TNetworkAddress& addr;
    const THdfsFileSplit& hdfs_file_split;
    bool is_cached;
    bool is_remote;
  };

  /// These functions are used as callbacks when processing the scheduling result. They
  /// will be called once per assigned scan range.
  typedef std::function<bool(const AssignmentInfo& assignment)> AssignmentFilter;
  typedef std::function<void(const AssignmentInfo& assignment)> AssignmentCallback;

 public:
  Result(const Plan& plan) : plan_(plan) {}

  /// Return the total number of scheduled assignments.
  int NumTotalAssignments() const { return CountAssignmentsIf(Any()); }

  /// Return the total number of assigned bytes.
  int NumTotalAssignedBytes() const { return CountAssignedBytesIf(Any()); }

  /// Return the number of scheduled assignments for a single host.
  int NumTotalAssignments(int host_idx) const {
    return CountAssignmentsIf(IsHost(host_idx));
  }

  /// Return the number of assigned bytes for a single host.
  int NumTotalAssignedBytes(int host_idx) const {
    return CountAssignedBytesIf(IsHost(host_idx));
  }

  /// Return the total number of assigned cached reads.
  int NumCachedAssignments() const { return CountAssignmentsIf(IsCached(Any())); }

  /// Return the total number of assigned bytes for cached reads.
  int NumCachedAssignedBytes() const { return CountAssignedBytesIf(IsCached(Any())); }

  /// Return the total number of assigned cached reads for a single host.
  int NumCachedAssignments(int host_idx) const {
    return CountAssignmentsIf(IsCached(IsHost(host_idx)));
  }

  /// Return the total number of assigned bytes for cached reads for a single host.
  int NumCachedAssignedBytes(int host_idx) const {
    return CountAssignedBytesIf(IsCached(IsHost(host_idx)));
  }

  /// Return the total number of assigned non-cached reads.
  int NumDiskAssignments() const { return CountAssignmentsIf(IsDisk(Any())); }

  /// Return the total number of assigned bytes for non-cached reads.
  int NumDiskAssignedBytes() const { return CountAssignedBytesIf(IsDisk(Any())); }

  /// Return the total number of assigned non-cached reads for a single host.
  int NumDiskAssignments(int host_idx) const {
    return CountAssignmentsIf(IsDisk(IsHost(host_idx)));
  }

  /// Return the total number of assigned bytes for non-cached reads for a single host.
  int NumDiskAssignedBytes(int host_idx) const {
    return CountAssignedBytesIf(IsDisk(IsHost(host_idx)));
  }

  /// Return the total number of assigned remote reads.
  int NumRemoteAssignments() const { return CountAssignmentsIf(IsRemote(Any())); }

  /// Return the total number of assigned bytes for remote reads.
  int NumRemoteAssignedBytes() const { return CountAssignedBytesIf(IsRemote(Any())); }

  /// Return the total number of assigned remote reads for a single host.
  int NumRemoteAssignments(int host_idx) const {
    return CountAssignmentsIf(IsRemote(IsHost(host_idx)));
  }

  /// Return the total number of assigned bytes for remote reads for a single host.
  int NumRemoteAssignedBytes(int host_idx) const {
    return CountAssignedBytesIf(IsRemote(IsHost(host_idx)));
  }

  /// Return the maximum number of assigned reads over all hosts.
  int MaxNumAssignmentsPerHost() const {
    NumAssignmentsPerBackend num_assignments_per_backend;
    CountAssignmentsPerBackend(&num_assignments_per_backend);
    int max_count = 0;
    for (const auto& elem: num_assignments_per_backend) {
      max_count = max(max_count, elem.second);
    }
    return max_count;
  }

  /// Return the maximum number of assigned reads over all hosts.
  int64_t MaxNumAssignedBytesPerHost() const {
    NumAssignedBytesPerBackend num_assigned_bytes_per_backend;
    CountAssignedBytesPerBackend(&num_assigned_bytes_per_backend);
    int64_t max_assigned_bytes = 0;
    for (const auto& elem: num_assigned_bytes_per_backend) {
      max_assigned_bytes = max(max_assigned_bytes, elem.second);
    }
    return max_assigned_bytes;
  }

  /// Return the minimum number of assigned reads over all hosts.
  /// NOTE: This is computed by traversing all recorded assignments and thus will not
  /// consider hosts without any assignments. Hence the minimum value to expect is 1 (not
  /// 0).
  int MinNumAssignmentsPerHost() const {
    NumAssignmentsPerBackend num_assignments_per_backend;
    CountAssignmentsPerBackend(&num_assignments_per_backend);
    int min_count = numeric_limits<int>::max();
    for (const auto& elem: num_assignments_per_backend) {
      min_count = min(min_count, elem.second);
    }
    DCHECK_GT(min_count, 0);
    return min_count;
  }

  /// Return the minimum number of assigned bytes over all hosts.
  /// NOTE: This is computed by traversing all recorded assignments and thus will not
  /// consider hosts without any assignments. Hence the minimum value to expect is 1 (not
  /// 0).
  int64_t MinNumAssignedBytesPerHost() const {
    NumAssignedBytesPerBackend num_assigned_bytes_per_backend;
    CountAssignedBytesPerBackend(&num_assigned_bytes_per_backend);
    int64_t min_assigned_bytes = 0;
    for (const auto& elem: num_assigned_bytes_per_backend) {
      min_assigned_bytes = max(min_assigned_bytes, elem.second);
    }
    DCHECK_GT(min_assigned_bytes, 0);
    return min_assigned_bytes;
  }

  /// Return the number of scan range assignments stored in this result.
  int NumAssignments() const { return assignments_.size(); }

  /// Return the number of distinct backends that have been picked by the scheduler so
  /// far.
  int NumDistinctBackends() const {
    unordered_set<IpAddr> backends;
    AssignmentCallback cb = [&backends](const AssignmentInfo& assignment) {
      backends.insert(assignment.addr.hostname);
    };
    ProcessAssignments(cb);
    return backends.size();
  }

  /// Return the full assignment for manual matching.
  const FragmentScanRangeAssignment& GetAssignment(int index = 0) const {
    DCHECK_GT(assignments_.size(), index);
    return assignments_[index];
  }

  /// Add an assignment to the result and return a reference, which can then be passed on
  /// to the scheduler.
  FragmentScanRangeAssignment* AddAssignment() {
    assignments_.push_back(FragmentScanRangeAssignment());
    return &assignments_.back();
  }

  /// Reset the result to an empty state.
  void Reset() { assignments_.clear(); }

 private:
  /// Vector to store results of consecutive scheduler runs.
  vector<FragmentScanRangeAssignment> assignments_;

  /// Reference to the plan, needed to look up hosts.
  const Plan& plan_;

  /// Dummy filter matching any assignment.
  AssignmentFilter Any() const {
    return [](const AssignmentInfo& assignment) { return true; };
  }

  /// Filter to only match assignments of a particular host.
  AssignmentFilter IsHost(int host_idx) const {
    TNetworkAddress expected_addr;
    plan_.cluster().GetBackendAddress(host_idx, &expected_addr);
    return [expected_addr](const AssignmentInfo& assignment) {
      return assignment.addr == expected_addr;
    };
  }

  /// Filter to only match assignments of cached reads.
  AssignmentFilter IsCached(AssignmentFilter filter) const {
    return [filter](const AssignmentInfo& assignment) {
        return filter(assignment) && assignment.is_cached;
    };
  }

  /// Filter to only match assignments of non-cached, local disk reads.
  AssignmentFilter IsDisk(AssignmentFilter filter) const {
    return [filter](const AssignmentInfo& assignment) {
        return filter(assignment) && !assignment.is_cached && !assignment.is_remote;
    };
  }

  /// Filter to only match assignments of remote reads.
  AssignmentFilter IsRemote(AssignmentFilter filter) const {
    return [filter](const AssignmentInfo& assignment) {
        return filter(assignment) && assignment.is_remote;
    };
  }

  /// Process all recorded assignments and call the supplied callback on each tuple of IP
  /// address and scan_range it iterates over.
  void ProcessAssignments(const AssignmentCallback& cb) const {
    for (const FragmentScanRangeAssignment& assignment: assignments_) {
      for (const auto& assignment_elem: assignment) {
        const TNetworkAddress& addr = assignment_elem.first;
        const PerNodeScanRanges& per_node_ranges = assignment_elem.second;
        for (const auto& per_node_ranges_elem: per_node_ranges) {
          const vector<TScanRangeParams> scan_range_params_vector =
              per_node_ranges_elem.second;
          for (const TScanRangeParams& scan_range_params: scan_range_params_vector) {
            const TScanRange& scan_range = scan_range_params.scan_range;
            DCHECK(scan_range.__isset.hdfs_file_split);
            const THdfsFileSplit& hdfs_file_split = scan_range.hdfs_file_split;
            bool is_cached = scan_range_params.__isset.is_cached
                ? scan_range_params.is_cached : false;
            bool is_remote = scan_range_params.__isset.is_remote
                ? scan_range_params.is_remote : false;
            cb({addr, hdfs_file_split, is_cached, is_remote});
          }
        }
      }
    }
  }

  /// Count all assignments matching the supplied filter callback.
  int CountAssignmentsIf(const AssignmentFilter& filter) const {
    int count = 0;
    AssignmentCallback cb = [&count, filter](const AssignmentInfo& assignment) {
      if (filter(assignment)) ++count;
    };
    ProcessAssignments(cb);
    return count;
  }

  /// Count all assignments matching the supplied filter callback.
  int64_t CountAssignedBytesIf(const AssignmentFilter& filter) const {
    int64_t assigned_bytes = 0;
    AssignmentCallback cb = [&assigned_bytes, filter](const AssignmentInfo& assignment) {
      if (filter(assignment)) assigned_bytes += assignment.hdfs_file_split.length;
    };
    ProcessAssignments(cb);
    return assigned_bytes;
  }

  /// Create a map containing the number of assigned scan ranges per node.
  void CountAssignmentsPerBackend(
      NumAssignmentsPerBackend* num_assignments_per_backend) const {
    AssignmentCallback cb = [&num_assignments_per_backend](
        const AssignmentInfo& assignment) {
      ++(*num_assignments_per_backend)[assignment.addr.hostname];
    };
    ProcessAssignments(cb);
  }

  /// Create a map containing the number of assigned bytes per node.
  void CountAssignedBytesPerBackend(
      NumAssignedBytesPerBackend* num_assignments_per_backend) const {
    AssignmentCallback cb = [&num_assignments_per_backend](
        const AssignmentInfo& assignment) {
      (*num_assignments_per_backend)[assignment.addr.hostname] +=
        assignment.hdfs_file_split.length;
    };
    ProcessAssignments(cb);
  }
};

/// This class wraps the SimpleScheduler and provides helper for easier instrumentation
/// during tests.
class SchedulerWrapper {
 public:
  SchedulerWrapper(const Plan& plan) : plan_(plan), metrics_("TestMetrics") {
    InitializeScheduler();
  }

  /// Call ComputeScanRangeAssignment().
  void Compute(Result* result) {
    DCHECK(scheduler_ != NULL);

    // Compute Assignment.
    FragmentScanRangeAssignment* assignment = result->AddAssignment();
    scheduler_->ComputeScanRangeAssignment(0, NULL, false,
        plan_.scan_range_locations(), plan_.referenced_datanodes(), false,
        plan_.query_options(), assignment);
  }

  /// Reset the state of the scheduler by re-creating and initializing it.
  void Reset() { InitializeScheduler(); }

  /// Methods to modify the internal lists of backends maintained by the scheduler.

  /// Add a backend to the scheduler.
  void AddBackend(const Host& host) {
    // Add to topic delta
    TTopicDelta delta;
    delta.topic_name = SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC;
    delta.is_delta = true;
    AddHostToTopicDelta(host, &delta);
    SendTopicDelta(delta);
  }

  /// Remove a backend from the scheduler.
  void RemoveBackend(const Host& host) {
    // Add deletion to topic delta
    TTopicDelta delta;
    delta.topic_name = SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC;
    delta.is_delta = true;
    delta.topic_deletions.push_back(host.ip);
    SendTopicDelta(delta);
  }

  /// Send a full map of the backends to the scheduler instead of deltas.
  void SendFullMembershipMap() {
    TTopicDelta delta;
    delta.topic_name = SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC;
    delta.is_delta = false;
    for (const Host& host: plan_.cluster().hosts()) {
      if (host.be_port >= 0) AddHostToTopicDelta(host, &delta);
    }
    SendTopicDelta(delta);
  }

  /// Send an empty update message to the scheduler.
  void SendEmptyUpdate() {
    TTopicDelta delta;
    delta.topic_name = SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC;
    delta.is_delta = true;
    SendTopicDelta(delta);
  }

 private:
  const Plan& plan_;
  boost::scoped_ptr<SimpleScheduler> scheduler_;
  MetricGroup metrics_;

  /// Initialize the internal scheduler object. The method uses the 'real' constructor
  /// used in the rest of the codebase, in contrast to the one that takes a list of
  /// backends, which is only used for testing purposes. This allows us to properly
  /// initialize the scheduler and exercise the UpdateMembership() method in tests.
  void InitializeScheduler() {
    DCHECK(scheduler_ == NULL);
    DCHECK_GT(plan_.cluster().NumHosts(), 0) << "Cannot initialize scheduler with 0 "
        << "hosts.";
    const Host& scheduler_host = plan_.cluster().hosts()[0];
    string scheduler_backend_id = scheduler_host.ip;
    TNetworkAddress scheduler_backend_address;
    scheduler_backend_address.hostname = scheduler_host.ip;
    scheduler_backend_address.port = scheduler_host.be_port;

    scheduler_.reset(new SimpleScheduler(NULL, scheduler_backend_id,
        scheduler_backend_address, &metrics_, NULL, NULL, NULL));
    scheduler_->Init();
    // Initialize the scheduler backend maps.
    SendFullMembershipMap();
  }

  /// Add a single host to the given TTopicDelta.
  void AddHostToTopicDelta(const Host& host, TTopicDelta* delta) const {
    DCHECK_GT(host.be_port, 0) << "Host cannot be added to scheduler without a running "
      << "backend";
    // Build backend descriptor.
    TBackendDescriptor be_desc;
    be_desc.address.hostname = host.ip;
    be_desc.address.port = host.be_port;
    be_desc.ip_address = host.ip;

    // Build topic item.
    TTopicItem item;
    item.key = host.ip;
    ThriftSerializer serializer(false);
    Status status = serializer.Serialize(&be_desc, &item.value);
    DCHECK(status.ok());

    // Add to topic delta.
    delta->topic_entries.push_back(item);
  }

  /// Send the given topic delta to the scheduler.
  void SendTopicDelta(const TTopicDelta& delta) {
    DCHECK(scheduler_ != NULL);
    // Wrap in topic delta map.
    StatestoreSubscriber::TopicDeltaMap delta_map;
    delta_map.emplace(SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC, delta);

    // Send to the scheduler.
    vector<TTopicDelta> dummy_result;
    scheduler_->UpdateMembership(delta_map, &dummy_result);
  }
};

class SchedulerTest : public testing::Test {
 protected:
  SchedulerTest() { srand(0); };
};

/// Smoke test to schedule a single table with a single scan range over a single host.
TEST_F(SchedulerTest, SingleHostSingleFile) {
  Cluster cluster;
  cluster.AddHost(true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1, ReplicaPlacement::LOCAL_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(1, result.NumTotalAssignments());
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(1, result.NumTotalAssignments(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(0, result.NumCachedAssignments());
}

/// Test scanning a simple table twice.
TEST_F(SchedulerTest, ScanTableTwice) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 2, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T");
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(4 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(4 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumCachedAssignedBytes());
}

// TODO: This test can be removed once we have the non-random backend round-robin by rank.
/// Schedule randomly over 3 backends and ensure that each backend is at least used once.
TEST_F(SchedulerTest, RandomReads) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  for (int i = 0; i < 100; ++i) scheduler.Compute(&result);

  ASSERT_EQ(100, result.NumAssignments());
  EXPECT_EQ(100, result.NumTotalAssignments());
  EXPECT_EQ(100 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(3, result.NumDistinctBackends());
  EXPECT_GE(result.MinNumAssignedBytesPerHost(), Block::DEFAULT_BLOCK_SIZE);
}

/// Distribute a table over the first 3 nodes in the cluster and verify that repeated
/// schedules always pick the first replica (random_replica = false).
TEST_F(SchedulerTest, LocalReadsPickFirstReplica) {
  Cluster cluster;
  for (int i = 0; i < 10; ++i) cluster.AddHost(i < 5, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(false);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  for (int i = 0; i < 3; ++i) scheduler.Compute(&result);

  EXPECT_EQ(3, result.NumTotalAssignments());
  EXPECT_EQ(3, result.NumDiskAssignments(0));
  EXPECT_EQ(0, result.NumDiskAssignments(1));
  EXPECT_EQ(0, result.NumDiskAssignments(2));
}

/// Verify that scheduling with random_replica = true results in a pseudo-random
/// round-robin selection of backends.
/// Disabled, global backend rotation not implemented.
TEST_F(SchedulerTest, DISABLED_RandomReplicaRoundRobin) {
  Cluster cluster;
  for (int i = 0; i < 10; ++i) cluster.AddHost(i < 5, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  for (int i = 0; i < 3; ++i) scheduler.Compute(&result);

  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(1));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(2));
}

/// Create a medium sized cluster with 100 nodes and compute a schedule over 3 tables.
TEST_F(SchedulerTest, TestMediumSizedCluster) {
  Cluster cluster;
  cluster.AddHosts(100, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::LOCAL_ONLY, 3);
  schema.AddMultiBlockTable("T2", 5, ReplicaPlacement::LOCAL_ONLY, 3);
  schema.AddMultiBlockTable("T3", 1, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.AddTableScan("T2");
  plan.AddTableScan("T3");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(16, result.NumTotalAssignments());
  EXPECT_EQ(16, result.NumDiskAssignments());
}

/// Verify that remote placement and scheduling work as expected.
TEST_F(SchedulerTest, RemoteOnlyPlacement) {
  Cluster cluster;
  for (int i = 0; i < 100; ++i) cluster.AddHost(i < 30, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::REMOTE_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(10, result.NumTotalAssignments());
  EXPECT_EQ(10, result.NumRemoteAssignments());
  EXPECT_EQ(Block::DEFAULT_BLOCK_SIZE, result.MaxNumAssignedBytesPerHost());
}

/// Add a table with 1000 scan ranges over 10 hosts and ensure that the right number of
/// assignments is computed.
TEST_F(SchedulerTest, ManyScanRanges) {
  Cluster cluster;
  cluster.AddHosts(10, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1000, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(1000, result.NumTotalAssignments());
  EXPECT_EQ(1000, result.NumDiskAssignments());
  // When distributing 1000 blocks with 1 replica over 10 hosts, the probability for the
  // most-picked host to end up with more than 140 blocks is smaller than 1E-3 (Chernoff
  // bound). Adding 2 additional replicas per block will make the probability even
  // smaller. This test is deterministic, so we expect a failure less often than every 1E3
  // changes to the test, not every 1E3 runs.
  EXPECT_LE(result.MaxNumAssignmentsPerHost(), 140);
  EXPECT_LE(result.MaxNumAssignedBytesPerHost(), 140 * Block::DEFAULT_BLOCK_SIZE);
}

/// Compute a schedule in a split cluster (disjoint set of backends and datanodes).
TEST_F(SchedulerTest, DisjointClusterWithRemoteReads) {
  Cluster cluster;
  for (int i = 0; i < 20; ++i) cluster.AddHost(i < 10, i >= 10);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::REMOTE_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);

  EXPECT_EQ(10, result.NumTotalAssignments());
  EXPECT_EQ(10, result.NumRemoteAssignments());
  // Expect that the datanodes were not mistaken for backends.
  for (int i = 10; i < 20; ++i) EXPECT_EQ(0, result.NumTotalAssignments(i));
}

/// Verify that cached replicas take precedence.
TEST_F(SchedulerTest, TestCachedReadPreferred) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 2}, {1});

  Plan plan(schema);
  // 1 of the 3 replicas is cached.
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes());
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes(1));
  EXPECT_EQ(0, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());

  // Compute additional assignments.
  for (int i = 0; i < 8; ++i) scheduler.Compute(&result);
  EXPECT_EQ(9 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes());
  EXPECT_EQ(9 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes(1));
  EXPECT_EQ(0, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());
}

/// Verify that disable_cached_reads is effective.
TEST_F(SchedulerTest, TestDisableCachedReads) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 2}, {1});

  Plan plan(schema);
  // 1 of the 3 replicas is cached.
  plan.AddTableScan("T1");
  plan.SetDisableCachedReads(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  scheduler.Compute(&result);
  EXPECT_EQ(0, result.NumCachedAssignedBytes());
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());

  // Compute additional assignments.
  for (int i = 0; i < 8; ++i) scheduler.Compute(&result);
  EXPECT_EQ(0, result.NumCachedAssignedBytes());
  EXPECT_EQ(9 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());
}

/// IMPALA-3019: Test for round robin reset problem. We schedule the same plan twice but
/// send an empty statestored message in between.
TEST_F(SchedulerTest, DISABLED_EmptyStatestoreMessage) {
  Cluster cluster;
  cluster.AddHosts(3, false, true);
  cluster.AddHost(true, false);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 1, ReplicaPlacement::RANDOM, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);

  scheduler.Compute(&result);
  EXPECT_EQ(1, result.NumDiskAssignments(0));
  EXPECT_EQ(0, result.NumDiskAssignments(1));
  result.Reset();

  scheduler.SendEmptyUpdate();
  scheduler.Compute(&result);
  EXPECT_EQ(0, result.NumDiskAssignments(0));
  EXPECT_EQ(1, result.NumDiskAssignments(1));
}

/// Test sending updates to the scheduler.
TEST_F(SchedulerTest, TestSendUpdates) {
  Cluster cluster;
  // 3 hosts, only first two run backends. This allows us to remove one of the backends
  // from the scheduler and then verify that reads are assigned to the other backend.
  for (int i=0; i < 3; ++i) cluster.AddHost(i < 2, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 1, ReplicaPlacement::RANDOM, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);

  scheduler.Compute(&result);
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(0));
  EXPECT_EQ(0, result.NumCachedAssignedBytes(0));
  EXPECT_EQ(0, result.NumRemoteAssignedBytes(0));
  EXPECT_EQ(0, result.NumDiskAssignedBytes(1));

  // Remove first host from scheduler.
  scheduler.RemoveBackend(cluster.hosts()[0]);
  result.Reset();

  scheduler.Compute(&result);
  EXPECT_EQ(0, result.NumDiskAssignedBytes(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(1));

  // Re-add first host from scheduler.
  scheduler.AddBackend(cluster.hosts()[0]);
  result.Reset();

  scheduler.Compute(&result);
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes(0));
  EXPECT_EQ(0, result.NumDiskAssignedBytes(1));
}

}  // end namespace impala

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  impala::CpuInfo::Init();
  impala::InitThreading();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
