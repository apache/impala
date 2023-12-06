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

#ifndef SCHEDULING_SCHEDULER_TEST_UTIL_H
#define SCHEDULING_SCHEDULER_TEST_UTIL_H

#include <string>
#include <unordered_map>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/scheduler.h"
#include "util/metrics.h"

namespace impala {

class ClusterMembershipMgr;
class HdfsFileSplitPB;
class Scheduler;
class TTopicDelta;

namespace test {

typedef std::string TableName;

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

/// Blocks and FileSplitGeneratorSpecs mimic real files, and for some parts of scheduling
/// (e.g. consistent remote scheduling) the actual file names and partition paths matter.
/// When defining a table, you can specify a block naming policy to control this.
///  - UNPARTITIONED means that the partition paths and partition ids are constant, but
///    the file names are unique.
///  - PARTITIONED_SINGLE_FILENAME means that the partition paths and partition ids are
///    unique, but the file name inside the partition is a single constant.
///  - PARTITIONED_UNIQUE_FILENAMES means that the partition paths, partition ids, and
///    the file names inside the partition.
/// These policies are mostly irrelevent for single block tables or tables with local
/// scheduling, so the default policy is UNPARTITIONED.
enum class BlockNamingPolicy {
  UNPARTITIONED,
  PARTITIONED_SINGLE_FILENAME,
  PARTITIONED_UNIQUE_FILENAMES,
};
std::ostream& operator<<(std::ostream& os, const BlockNamingPolicy& naming_policy);

/// Host model. Each host can have either a backend, a datanode, or both. To specify that
/// a host should not act as a backend or datanode specify '-1' as the respective port.
/// A host with a backend is always a coordinator but it may not be an executor.
struct Host {
  Host(const Hostname& name, const IpAddr& ip, int be_port, int dn_port, bool is_executor)
    : name(name), ip(ip), be_port(be_port), dn_port(dn_port), is_coordinator(true),
      is_executor(is_executor) {}
  Hostname name;
  IpAddr ip;
  int be_port; // Backend port
  int dn_port; // Datanode port
  bool is_coordinator; // True if this is a coordinator host
  bool is_executor; // True if this is an executor host
};

/// A cluster stores a list of hosts and provides various methods to add hosts to the
/// cluster. All hosts are guaranteed to have unique IP addresses and hostnames.
class Cluster {
 public:
  /// Add a host and return the host's index. 'hostname' and 'ip' of the new host will be
  /// generated and are guaranteed to be unique.
  /// TODO: Refactor the construction of a host and its addition to a cluster to
  /// avoid the boolean input parameters.
  int AddHost(bool has_backend, bool has_datanode, bool is_executor = true);

  /// Add a number of hosts with the same properties by repeatedly calling AddHost(..).
  void AddHosts(int num_hosts, bool has_backend, bool has_datanode,
      bool is_executor = true);

  /// Return the backend address (ip, port) for the host with index 'host_idx'.
  void GetBackendAddress(int host_idx, TNetworkAddress* addr) const;

  const std::vector<Host>& hosts() const { return hosts_; }
  int NumHosts() const { return hosts_.size(); }

  /// Helper function to create a cluster with Impala nodes separate from the datanodes.
  /// This is primarily used for consistent remote scheduling tests. This places
  /// the impalad nodes first, then the data nodes. Impalad nodes will have indices
  /// in the range [0, num_impala_nodes-1] and data nodes will have indices in the
  /// range [num_impala_nodes, num_impala_nodes+num_data_nodes-1].
  static Cluster CreateRemoteCluster(int num_impala_nodes, int num_data_nodes);

  /// These methods return lists of host indexes, grouped by their type, which can be used
  /// to draw samples of random sets of hosts.
  /// TODO: Think of a nicer abstraction to expose this information.
  const std::vector<int>& backend_host_idxs() const { return backend_host_idxs_; }
  const std::vector<int>& datanode_host_idxs() const { return datanode_host_idxs_; }
  const std::vector<int>& datanode_with_backend_host_idxs() const;
  const std::vector<int>& datanode_only_host_idxs() const;

 private:
  /// Port for all backends.
  static const int BACKEND_PORT;

  /// Port for all datanodes.
  static const int DATANODE_PORT;

  /// Prefix for all generated hostnames.
  static const std::string HOSTNAME_PREFIX;

  /// First octet for all generated IP addresses.
  static const std::string IP_PREFIX;

  /// List of hosts in this cluster.
  std::vector<Host> hosts_;

  /// Lists of indexes of hosts, grouped by their type. The lists reference hosts in
  /// 'hosts_' by index and are used for random sampling.
  ///
  /// All hosts with a backend.
  std::vector<int> backend_host_idxs_;
  /// All hosts with a datanode.
  std::vector<int> datanode_host_idxs_;
  /// All hosts with a datanode and a backend.
  std::vector<int> datanode_with_backend_host_idxs_;
  /// All hosts with a datanode but no backend.
  std::vector<int> datanode_only_host_idxs_;

  /// Map from IP addresses to host indexes.
  std::unordered_map<IpAddr, int> ip_to_idx_;
};

struct Block {
  /// By default all blocks are of the same size.
  int64_t length = DEFAULT_BLOCK_SIZE;

  /// Index into the cluster that owns the table that owns this block.
  std::vector<int> replica_host_idxs;

  /// Flag for each entry in replica_host_idxs whether it is a cached replica or not.
  std::vector<bool> replica_host_idx_is_cached;

  /// Default size for new blocks.
  static const int64_t DEFAULT_BLOCK_SIZE;
};

struct FileSplitGeneratorSpec {
  FileSplitGeneratorSpec() {}
  FileSplitGeneratorSpec(
      int64_t length, int64_t block, bool splittable, bool is_footer_only = false)
    : length(length),
      block_size(block),
      is_splittable(splittable),
      is_footer_only(is_footer_only) {}

  /// Length of file for which to generate file splits.
  int64_t length = DEFAULT_FILE_SIZE;

  /// Size of each split.
  int64_t block_size = DEFAULT_BLOCK_SIZE;

  bool is_splittable = true;

  bool is_footer_only = false;

  static const int64_t DEFAULT_FILE_SIZE;
  static const int64_t DEFAULT_BLOCK_SIZE;
};

/// A table models multiple files. Each file can be represented explicitly with a Block
/// or with a FileSplitGeneratorSpecs. A table can consist of files with both
/// representations. The table can specify a BlockNamingPolicy, which tailors the
/// file name and path for scan ranges to simulate partitioned vs unpartitioned tables.
/// Consistent remote scheduling depends on the file paths, but the file names do not
/// impact other aspects of scheduling.
struct Table {
  BlockNamingPolicy naming_policy = BlockNamingPolicy::UNPARTITIONED;
  std::vector<Block> blocks;
  std::vector<FileSplitGeneratorSpec> specs;
};

class Schema {
 public:
  Schema(const Cluster& cluster) : cluster_(cluster) {}

  /// Add a table with no blocks. This is useful for tables with a custom naming
  /// policy that later add FileSplitGeneratorSpecs.
  void AddEmptyTable(const TableName& table_name, BlockNamingPolicy naming_policy);

  /// Add a table consisting of a single block to the schema with explicitly specified
  /// replica indexes for non-cached replicas and without any cached replicas. Replica
  /// indexes must refer to hosts in cluster_.hosts() by index.
  void AddSingleBlockTable(
      const TableName& table_name, const std::vector<int>& non_cached_replica_host_idxs);

  /// Add a table consisting of a single block to the schema with explicitly specified
  /// replica indexes for both non-cached and cached replicas. Values in both lists must
  /// refer to hosts in cluster_.hosts() by index. Both lists must be disjoint, i.e., a
  /// replica can either be cached or not.
  void AddSingleBlockTable(const TableName& table_name,
      const std::vector<int>& non_cached_replica_host_idxs,
      const std::vector<int>& cached_replica_host_idxs);

  /// Add a table to the schema, selecting replica hosts according to the given replica
  /// placement preference. All replicas will be non-cached.
  void AddMultiBlockTable(const TableName& table_name, int num_blocks,
      ReplicaPlacement replica_placement, int num_replicas);

  /// Add a table to the schema, selecting replica hosts according to the given replica
  /// placement preference. After replica selection has been done, 'num_cached_replicas'
  /// of them are marked as cached. The table uses the specified 'naming_policy' for
  /// its blocks.
  void AddMultiBlockTable(const TableName& table_name, int num_blocks,
      ReplicaPlacement replica_placement, int num_replicas, int num_cached_replicas,
      BlockNamingPolicy naming_policy);

  /// Adds FileSplitGeneratorSpecs to table named 'table_name'. If the table does not
  /// exist, creates a new table. Otherwise, adds the 'specs' to an existing table.
  void AddFileSplitGeneratorSpecs(
      const TableName& table_name, const std::vector<FileSplitGeneratorSpec>& specs);

  /// Adds 'num' default FileSplitGeneratorSpecs to table named 'table_name'. If the table
  /// does not exist, creates a new table. Otherwise, adds the 'specs' to an existing
  /// table.
  void AddFileSplitGeneratorDefaultSpecs(const TableName& table_name, int num);

  const Table& GetTable(const TableName& table_name) const;

  const Cluster& cluster() const { return cluster_; }

 private:
  /// Store a reference to the cluster, from which hosts are sampled. Test results will
  /// use the cluster to resolve host indexes to hostnames and IP addresses.
  const Cluster& cluster_;

  std::unordered_map<TableName, Table> tables_;
};

/// Plan model. A plan contains a list of tables to scan and the query options to be used
/// during scheduling.
class Plan {
 public:
  Plan(const Schema& schema) : schema_(schema) {}

  const TQueryOptions& query_options() const { return query_options_; }

  void SetReplicaPreference(TReplicaPreference::type p);

  void SetRandomReplica(bool b) { query_options_.schedule_random_replica = b; }
  void SetNumRemoteExecutorCandidates(int32_t num);
  const Cluster& cluster() const { return schema_.cluster(); }

  const std::vector<TNetworkAddress>& referenced_datanodes() const;

  const TScanRangeSpec& scan_range_specs() const;

  /// Add a scan across all coordinators.
  void AddSystemTableScan();

  /// Add a scan of table 'table_name' to the plan. This method will populate the internal
  /// TScanRangeSpecs and can be called multiple times for the same table to schedule
  /// additional scans.
  void AddTableScan(const TableName& table_name);

 private:
  /// Store a reference to the schema, from which scanned tables will be read.
  const Schema& schema_;

  TQueryOptions query_options_;

  /// List of all datanodes that are referenced by this plan. Only hosts that have an
  /// assigned scan range are added here.
  std::vector<TNetworkAddress> referenced_datanodes_;

  /// Map from plan host index to an index in 'referenced_datanodes_'.
  std::unordered_map<int, int> host_idx_to_datanode_idx_;

  /// Scan range specs that are scheduled by the Scheduler.
  TScanRangeSpec scan_range_specs_;

  /// Initialize a TScanRangeLocationList object in place.
  void BuildTScanRangeLocationList(const TableName& table_name, const Block& block,
      int block_idx, BlockNamingPolicy naming_policy,
      TScanRangeLocationList* scan_range_locations);

  /// Builds appropriate paths for a Block given the table name and block naming
  /// policy.
  void GetBlockPaths(const TableName& table_name, bool is_spec, int index,
      BlockNamingPolicy naming_policy, string* relative_path, int64_t* partition_id,
      string* partition_path);

  /// Initializes a scan range for a Block.
  void BuildScanRange(
      const TableName& table_name, const Block& block, int block_idx,
      BlockNamingPolicy naming_policy, TScanRange* range);

  /// Initializes a scan range for a FileSplitGeneratorSpec.
  void BuildScanRangeSpec(const TableName& table_name, const FileSplitGeneratorSpec& spec,
      int spec_idx, BlockNamingPolicy naming_policy,
      TFileSplitGeneratorSpec* thrift_spec);

  /// Look up the plan-local host index of 'cluster_datanode_idx'. If the host has not
  /// been added to the plan before, it will add it to 'referenced_datanodes_' and return
  /// the new index.
  int FindOrInsertDatanodeIndex(int cluster_datanode_idx);
};

class Result {
 private:
  /// Map to count the number of assignments per backend.
  typedef std::unordered_map<IpAddr, int> NumAssignmentsPerBackend;

  /// Map to count the number of assigned bytes per backend.
  typedef std::unordered_map<IpAddr, int64_t> NumAssignedBytesPerBackend;

  /// Parameter type for callbacks, which are used to filter scheduling results.
  struct AssignmentInfo {
    const NetworkAddressPB& addr;
    const HdfsFileSplitPB& hdfs_file_split;
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
  int NumTotalAssignments(int host_idx) const;

  /// Return the number of assigned bytes for a single host.
  int NumTotalAssignedBytes(int host_idx) const;

  /// Return the total number of assigned cached reads.
  int NumCachedAssignments() const { return CountAssignmentsIf(IsCached(Any())); }

  /// Return the total number of assigned bytes for cached reads.
  int NumCachedAssignedBytes() const { return CountAssignedBytesIf(IsCached(Any())); }

  /// Return the total number of assigned cached reads for a single host.
  int NumCachedAssignments(int host_idx) const;

  /// Return the total number of assigned bytes for cached reads for a single host.
  int NumCachedAssignedBytes(int host_idx) const;

  /// Return the total number of assigned non-cached reads.
  int NumDiskAssignments() const { return CountAssignmentsIf(IsDisk(Any())); }

  /// Return the total number of assigned bytes for non-cached reads.
  int NumDiskAssignedBytes() const { return CountAssignedBytesIf(IsDisk(Any())); }

  /// Return the total number of assigned non-cached reads for a single host.
  int NumDiskAssignments(int host_idx) const;

  /// Return the total number of assigned bytes for non-cached reads for a single host.
  int NumDiskAssignedBytes(int host_idx) const;

  /// Return the total number of assigned remote reads.
  int NumRemoteAssignments() const { return CountAssignmentsIf(IsRemote(Any())); }

  /// Return the total number of assigned bytes for remote reads.
  int NumRemoteAssignedBytes() const { return CountAssignedBytesIf(IsRemote(Any())); }

  /// Return the total number of assigned remote reads for a single host.
  int NumRemoteAssignments(int host_idx) const;

  /// Return the total number of assigned bytes for remote reads for a single host.
  int NumRemoteAssignedBytes(int host_idx) const;

  /// Return the maximum number of assigned reads over all hosts.
  int MaxNumAssignmentsPerHost() const;

  /// Return the maximum number of assigned reads over all hosts.
  int64_t MaxNumAssignedBytesPerHost() const;

  /// Return the minimum number of assigned reads over all hosts.
  /// NOTE: This is computed by traversing all recorded assignments and thus will not
  /// consider hosts without any assignments. Hence the minimum value to expect is 1 (not
  /// 0).
  int MinNumAssignmentsPerHost() const;

  /// Return the minimum number of assigned bytes over all hosts.
  /// NOTE: This is computed by traversing all recorded assignments and thus will not
  /// consider hosts without any assignments. Hence the minimum value to expect is 1 (not
  /// 0).
  int64_t MinNumAssignedBytesPerHost() const;

  /// Return the number of scan range assignments stored in this result.
  int NumAssignments() const { return assignments_.size(); }

  /// Return the number of distinct backends that have been picked by the scheduler so
  /// far.
  int NumDistinctBackends() const;

  /// Return the full assignment for manual matching.
  const FragmentScanRangeAssignment& GetAssignment(int index = 0) const;

  /// Add an assignment to the result and return a reference, which can then be passed on
  /// to the scheduler.
  FragmentScanRangeAssignment* AddAssignment();

  /// Reset the result to an empty state.
  void Reset() { assignments_.clear(); }

 private:
  /// Vector to store results of consecutive scheduler runs.
  std::vector<FragmentScanRangeAssignment> assignments_;

  /// Reference to the plan, needed to look up hosts.
  const Plan& plan_;

  /// Dummy filter matching any assignment.
  AssignmentFilter Any() const;

  /// Filter to only match assignments of a particular host.
  AssignmentFilter IsHost(int host_idx) const;

  /// Filter to only match assignments of cached reads.
  AssignmentFilter IsCached(AssignmentFilter filter) const;

  /// Filter to only match assignments of non-cached, local disk reads.
  AssignmentFilter IsDisk(AssignmentFilter filter) const;

  /// Filter to only match assignments of remote reads.
  AssignmentFilter IsRemote(AssignmentFilter filter) const;

  /// Process all recorded assignments and call the supplied callback on each tuple of IP
  /// address and scan_range it iterates over.
  void ProcessAssignments(const AssignmentCallback& cb) const;

  /// Count all assignments matching the supplied filter callback.
  int CountAssignmentsIf(const AssignmentFilter& filter) const;

  /// Count all assignments matching the supplied filter callback.
  int64_t CountAssignedBytesIf(const AssignmentFilter& filter) const;

  /// Create a map containing the number of assigned scan ranges per node.
  void CountAssignmentsPerBackend(
      NumAssignmentsPerBackend* num_assignments_per_backend) const;

  /// Create a map containing the number of assigned bytes per node.
  void CountAssignedBytesPerBackend(
      NumAssignedBytesPerBackend* num_assignments_per_backend) const;
};

/// This class wraps the Scheduler and provides helper for easier instrumentation
/// during tests.
class SchedulerWrapper {
 public:
  SchedulerWrapper(const Plan& plan);

  /// Call ComputeScanRangeAssignment() with exec_at_coord set to false.
  Status Compute(Result* result, bool include_all_coordinators = false) {
    return Compute(false, result, include_all_coordinators);
  }

  /// Call ComputeScanRangeAssignment().
  Status Compute(bool exec_at_coord, Result* result,
      bool include_all_coordinators = false);

  /// Reset the state of the scheduler by re-creating and initializing it.
  void Reset() { InitializeScheduler(); }

  /// Methods to modify the internal lists of backends maintained by the scheduler.

  /// Add a backend to the scheduler.
  void AddBackend(const Host& host);

  /// Remove a backend from the scheduler.
  void RemoveBackend(const Host& host);

  /// Send a full map of the backends to the scheduler instead of deltas.
  void SendFullMembershipMap();

  /// Send an empty update message to the scheduler.
  void SendEmptyUpdate();

 private:
  const Plan& plan_;
  boost::scoped_ptr<ClusterMembershipMgr> cluster_membership_mgr_;
  boost::scoped_ptr<Scheduler> scheduler_;
  MetricGroup metrics_;

  /// Initialize the internal scheduler object. The method uses the 'real' constructor
  /// used in the rest of the codebase, in contrast to the one that takes a list of
  /// backends, which is only used for testing purposes. This allows us to properly
  /// initialize the scheduler and exercise the UpdateMembership() method in tests.
  void InitializeScheduler();

  /// Add a single host to the given TTopicDelta.
  void AddHostToTopicDelta(const Host& host, TTopicDelta* delta) const;

  /// Send the given topic delta to the scheduler.
  void SendTopicDelta(const TTopicDelta& delta);
};

} // end namespace test
} // end namespace impala

#endif
