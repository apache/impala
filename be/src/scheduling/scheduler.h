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

#pragma once

#include <random>
#include <string>
#include <vector>
#include <boost/heap/binomial_heap.hpp>
#include <boost/unordered_map.hpp>
#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "scheduling/executor-group.h"
#include "scheduling/schedule-state.h"
#include "util/metrics-fwd.h"
#include "util/network-util.h"
#include "util/runtime-profile.h"

namespace impala {

class BackendDescriptorPB;
class MetricGroup;
class RequestPoolService;
class TPlanExecInfo;
class TQueryOptions;
class TScanRangeLocationList;

namespace test {
class SchedulerWrapper;
}

/// Performs simple scheduling by matching between a list of executor backends that is
/// supplied by the users of this class, and a list of target data locations.
///
/// TODO: Track assignments (assignment_ctx in ComputeScanRangeAssignment) per query
///       instead of per plan node?
/// TODO: Inject global dependencies into the class (for example ExecEnv::GetInstance(),
///       RNG used during scheduling, FLAGS_*)
///       to make it testable.
/// TODO: Extend the benchmarks of the scheduler. The tests need to include setups with:
///         - Scheduling query plans with concurrent updates to the internal backend
///           configuration.
class Scheduler {
 public:
  static const std::string PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION;

  Scheduler(MetricGroup* metrics, RequestPoolService* request_pool_service);

  /// Current snapshot of executors to be used for scheduling a scan.
  struct ExecutorConfig {
    const ExecutorGroup& group;
    const BackendDescriptorPB& coord_desc;
    const ExecutorGroup& all_coords;
  };

  /// Populates given query schedule and assigns fragments to hosts based on scan
  /// ranges in the query exec request. 'executor_config' must contain a non-empty group
  /// unless IsCoordinatorOnlyQuery() is true.
  Status Schedule(const ExecutorConfig& executor_config, ScheduleState* state);

  /// Returns true if the query is only supposed to run on the coordinator (single
  /// unpartitioned fragment).
  bool IsCoordinatorOnlyQuery(const TQueryExecRequest& exec_request);

 private:
  /// Map from a host's IP address to the next executor to be round-robin scheduled for
  /// that host (needed for setups with multiple executors on a single host)
  typedef boost::unordered_map<IpAddr, ExecutorGroup::Executors::const_iterator>
      NextExecutorPerHost;

  /// Map from file paths to hosts where those files are scheduled, grouped by scan node
  /// ID.
  typedef std::unordered_map<int, std::unordered_map<
      std::string, std::unordered_set<NetworkAddressPB>>> ByNodeFilepathToHosts;

  /// Internal structure to track scan range assignments for an executor host. This struct
  /// is used as the heap element in and maintained by AddressableAssignmentHeap.
  struct ExecutorAssignmentInfo {
    /// The number of bytes assigned to an executor.
    int64_t assigned_bytes;

    /// Each host gets assigned a random rank to break ties in a random but deterministic
    /// order per plan node.
    const int random_rank;

    /// IP address of the executor.
    IpAddr ip;

    /// Compare two elements of this struct. The key is (assigned_bytes, random_rank).
    bool operator>(const ExecutorAssignmentInfo& rhs) const {
      if (assigned_bytes != rhs.assigned_bytes) {
        return assigned_bytes > rhs.assigned_bytes;
      }
      return random_rank > rhs.random_rank;
    }
  };

  /// Heap to compute candidates for scan range assignments. Elements are of type
  /// ExecutorAssignmentInfo and track assignment information for each executor. By
  /// default boost implements a max-heap so we use std::greater<T> to obtain a min-heap.
  /// This will make the top() element of the heap be the executor with the lowest number
  /// of assigned bytes and the lowest random rank.
  typedef boost::heap::binomial_heap<ExecutorAssignmentInfo,
      boost::heap::compare<std::greater<ExecutorAssignmentInfo>>>
      AssignmentHeap;

  /// Map to look up handles to heap elements to modify heap element keys.
  typedef boost::unordered_map<IpAddr, AssignmentHeap::handle_type> ExecutorHandleMap;

  /// Class to store executor information in an addressable heap. In addition to
  /// AssignmentHeap it can be used to look up heap elements by their IP address and
  /// update their key. For each plan node we create a new heap, so they are not shared
  /// between concurrent invocations of the scheduler.
  class AddressableAssignmentHeap {
   public:
    const AssignmentHeap& executor_heap() const { return executor_heap_; }
    const ExecutorHandleMap& executor_handles() const { return executor_handles_; }

    void InsertOrUpdate(const IpAddr& ip, int64_t assigned_bytes, int rank);

    // Forward interface for boost::heap
    decltype(auto) size() const { return executor_heap_.size(); }
    decltype(auto) top() const { return executor_heap_.top(); }

    // Forward interface for boost::unordered_map
    decltype(auto) find(const IpAddr& ip) const { return executor_handles_.find(ip); }
    decltype(auto) end() const { return executor_handles_.end(); }

   private:
    // Heap to determine next executor.
    AssignmentHeap executor_heap_;
    // Maps executor IPs to handles in the heap.
    ExecutorHandleMap executor_handles_;
  };

  /// Class to store context information on assignments during scheduling. It is
  /// initialized with a copy of the executor group and assigns a random rank to each
  /// executor to break ties in cases where multiple executors have been assigned the same
  /// number or bytes. It tracks the number of assigned bytes, which executors have
  /// already been used, etc. Objects of this class are created in
  /// ComputeScanRangeAssignment() and thus don't need to be thread safe.
  class AssignmentCtx {
   public:
    AssignmentCtx(const ExecutorGroup& executor_group, IntCounter* total_assignments,
        IntCounter* total_local_assignments, std::mt19937* rng);

    /// Among hosts in 'data_locations', select the one with the minimum number of
    /// assigned bytes. If executors have been assigned equal amounts of work and
    /// 'break_ties_by_rank' is true, then the executor rank is used to break ties.
    /// Otherwise the first executor according to their order in 'data_locations' is
    /// selected.
    const IpAddr* SelectExecutorFromCandidates(
        const std::vector<IpAddr>& data_locations, bool break_ties_by_rank);

    /// Populate 'remote_executor_candidates' with 'num_candidates' distinct
    /// executors. The algorithm for picking remote executor candidates is to hash
    /// the file name / offset from 'hdfs_file_split' multiple times and look up the
    /// closest executors stored in the ExecutorGroup's HashRing. Given the same file
    /// name / offset and same set of executors, this function is deterministic. The hash
    /// ring also limits the disruption when executors are added or removed. Note that
    /// 'num_candidates' cannot be 0 and must be less than the total number of executors.
    void GetRemoteExecutorCandidates(const THdfsFileSplit* hdfs_file_split,
        int num_remote_replicas, vector<IpAddr>* remote_executor_candidates);

    /// Select an executor for a remote read. If there are unused executor hosts, then
    /// those will be preferred. Otherwise the one with the lowest number of assigned
    /// bytes is picked. If executors have been assigned equal amounts of work, then the
    /// executor rank is used to break ties.
    const IpAddr* SelectRemoteExecutor();

    /// Return the next executor that has not been assigned to. This assumes that a
    /// returned executor will also be assigned to. The caller must make sure that
    /// HasUnusedExecutors() is true.
    const IpAddr* GetNextUnusedExecutorAndIncrement();

    /// Pick an executor in round-robin fashion from multiple executors on a single host.
    void SelectExecutorOnHost(const IpAddr& executor_ip, BackendDescriptorPB* executor);

    /// Build a new ScanRangeParamsPB object and append it to the assignment list for the
    /// tuple (executor, node_id) in 'assignment'. Also, update assignment_heap_ and
    /// assignment_byte_counters_, increase the counters 'total_assignments_' and
    /// 'total_local_assignments_'. 'scan_range_locations' contains information about the
    /// scan range and its replica locations.
    void RecordScanRangeAssignment(const BackendDescriptorPB& executor,
        PlanNodeId node_id, const vector<TNetworkAddress>& host_list,
        const TScanRangeLocationList& scan_range_locations,
        FragmentScanRangeAssignment* assignment);

    const ExecutorGroup& executor_group() const { return executor_group_; }

    /// Print the assignment and statistics to VLOG_FILE.
    void PrintAssignment(const FragmentScanRangeAssignment& assignment);

   private:
    /// A struct to track various counts of assigned bytes during scheduling.
    struct AssignmentByteCounters {
      int64_t remote_bytes = 0;
      int64_t local_bytes = 0;
      int64_t cached_bytes = 0;
    };

    /// Used to look up hostnames to IP addresses and IP addresses to executors.
    const ExecutorGroup& executor_group_;

    // Addressable heap to select remote executors from. Elements are ordered by the
    // number of already assigned bytes (and a random rank to break ties).
    AddressableAssignmentHeap assignment_heap_;

    /// Store a random rank per executor host to break ties between otherwise equivalent
    /// replicas (e.g., those having the same number of assigned bytes).
    boost::unordered_map<IpAddr, int> random_executor_rank_;

    /// Index into random_executor_order. It points to the first unused executor and is
    /// used to select unused executors and inserting them into the assignment_heap_.
    int first_unused_executor_idx_;

    /// Store a random permutation of executor hosts to select executors from.
    std::vector<IpAddr> random_executor_order_;

    /// Track round robin information per executor host.
    NextExecutorPerHost next_executor_per_host_;

    /// Track number of assigned bytes that have been read from cache, locally, or
    /// remotely.
    AssignmentByteCounters assignment_byte_counters_;

    /// Pointers to the scheduler's counters.
    IntCounter* total_assignments_;
    IntCounter* total_local_assignments_;

    /// Return whether there are executors that have not been assigned a scan range.
    bool HasUnusedExecutors() const;

    /// Return the rank of an executor.
    int GetExecutorRank(const IpAddr& ip) const;
  };

  /// Total number of scan ranges assigned to executors during the lifetime of the
  /// scheduler.
  int64_t num_assignments_;

  /// MetricGroup subsystem access
  MetricGroup* metrics_;

  /// Locality metrics
  IntCounter* total_assignments_ = nullptr;
  IntCounter* total_local_assignments_ = nullptr;

  /// Initialization metric
  BooleanProperty* initialized_ = nullptr;

  /// Used for user-to-pool resolution and looking up pool configurations. Not owned by
  /// us.
  RequestPoolService* request_pool_service_;

  /// Returns the backend descriptor corresponding to 'host' which could be a remote
  /// backend or the local host itself. The returned descriptor should not be retained
  /// beyond the lifetime of 'executor_config'.
  const BackendDescriptorPB& LookUpBackendDesc(
      const ExecutorConfig& executor_config, const NetworkAddressPB& host);

  /// Returns the KRPC host in 'executor_config' based on the thrift backend address
  /// 'backend_host'. Will DCHECK if the KRPC address is not valid.
  NetworkAddressPB LookUpKrpcHost(
      const ExecutorConfig& executor_config, const NetworkAddressPB& backend_host);

  /// Determine the pool for a user and query options via request_pool_service_.
  Status GetRequestPool(const std::string& user, const TQueryOptions& query_options,
      std::string* pool) const;

  /// Generates scan ranges from 'specs' and places them in 'generated_scan_ranges'.
  Status GenerateScanRanges(const std::vector<TFileSplitGeneratorSpec>& specs,
      std::vector<TScanRangeLocationList>* generated_scan_ranges);

  /// Compute the assignment of scan ranges to hosts for each scan node in
  /// the schedule's TQueryExecRequest.plan_exec_info.
  /// Unpartitioned fragments are assigned to the coordinator. Populate the schedule's
  /// fragment_exec_params_ with the resulting scan range assignment.
  /// We have a benchmark for this method in be/src/benchmarks/scheduler-benchmark.cc.
  /// 'executor_config' is the executor configuration to use for scheduling.
  Status ComputeScanRangeAssignment(
      const ExecutorConfig& executor_config, ScheduleState* state);

  /// Process the list of scan ranges of a single plan node and compute scan range
  /// assignments (returned in 'assignment'). The result is a mapping from hosts to their
  /// assigned scan ranges per plan node. Inputs that are scan range specs are used to
  /// generate scan ranges.
  ///
  /// If exec_at_coord is true, all scan ranges will be assigned to the coordinator host.
  /// Otherwise the assignment is computed for each scan range as follows:
  ///
  /// Scan ranges refer to data, which is usually replicated on multiple hosts. All scan
  /// ranges where one of the replica hosts also runs an impala executor are processed
  /// first. If more than one of the replicas run an impala executor, then the 'memory
  /// distance' of each executor is considered. The concept of memory distance reflects
  /// the cost of moving data into the processing executor's main memory. Reading from
  /// cached replicas is generally considered less costly than reading from a local disk,
  /// which in turn is cheaper than reading data from a remote node. If multiple executors
  /// of the same memory distance are found, then the one with the least amount of
  /// previously assigned work is picked, thus aiming to distribute the work as evenly as
  /// possible.
  ///
  /// Finally, scan ranges are considered which do not have an impalad executor running on
  /// any of their data nodes. They will be load-balanced by assigned bytes across all
  /// executors.
  ///
  /// The resulting assignment is influenced by the following query options:
  ///
  /// replica_preference:
  ///   This value is used as a minimum memory distance for all replicas. For example, by
  ///   setting this to DISK_LOCAL, all cached replicas will be treated as if they were
  ///   not cached, but local disk replicas. This can help prevent hot-spots by spreading
  ///   the assignments over more replicas. Allowed values are CACHE_LOCAL (default),
  ///   DISK_LOCAL and REMOTE.
  ///
  /// schedule_random_replica:
  ///   When equivalent executors with a memory distance of DISK_LOCAL are found for a
  ///   scan range (same memory distance, same amount of assigned work), then the first
  ///   one will be picked deterministically. This aims to make better use of OS buffer
  ///   caches, but can lead to performance bottlenecks on individual hosts. Setting this
  ///   option to true will randomly change the order in which equivalent replicas are
  ///   picked for different plan nodes. This helps to compute a more even assignment,
  ///   with the downside being an increased memory usage for OS buffer caches. The
  ///   default setting is false. Selection between equivalent replicas with memory
  ///   distance of CACHE_LOCAL or REMOTE happens based on a random order.
  ///
  /// The method takes the following parameters:
  ///
  /// executor_config:          Executor configuration to use for scheduling.
  /// node_id:                 ID of the plan node.
  /// node_replica_preference: Query hint equivalent to replica_preference.
  /// node_random_replica:     Query hint equivalent to schedule_random_replica.
  /// locations:               List of scan ranges to be assigned to executors.
  /// host_list:               List of hosts, into which 'locations' will index.
  /// exec_at_coord:           Whether to schedule all scan ranges on the coordinator.
  /// query_options:           Query options for the current query.
  /// timer:                   Tracks execution time of ComputeScanRangeAssignment.
  /// rng:                     Random number generated used for any random decisions
  /// summary_profile:         Summary profile for any scheduler warnings.
  /// assignment:              Output parameter, to which new assignments will be added.
  Status ComputeScanRangeAssignment(const ExecutorConfig& executor_config,
      PlanNodeId node_id, const TReplicaPreference::type* node_replica_preference,
      bool node_random_replica, const std::vector<TScanRangeLocationList>& locations,
      const std::vector<TNetworkAddress>& host_list, bool exec_at_coord,
      const TQueryOptions& query_options, RuntimeProfile::Counter* timer,
      std::mt19937* rng, RuntimeProfile* summary_profile,
      FragmentScanRangeAssignment* assignment);

  /// Computes execution parameters for all backends assigned in the query and always one
  /// for the coordinator backend since it participates in execution regardless. Must be
  /// called after ComputeFragmentExecParams().
  void ComputeBackendExecParams(
      const ExecutorConfig& executor_config, ScheduleState* state);

  /// Compute the per-fragment execution parameters for all plans in the schedule's
  /// TQueryExecRequest.plan_exec_info.
  /// This includes the routing information (destinations, per_exch_num_senders,
  /// sender_id)
  /// 'executor_config' is the executor configuration to use for scheduling.
  Status ComputeFragmentExecParams(
      const ExecutorConfig& executor_config, ScheduleState* state);

  /// Recursively create FInstanceScheduleState and set per_node_scan_ranges for
  /// fragment_state and its input fragments via a depth-first traversal.
  /// All fragments are part of plan_exec_info.
  Status ComputeFragmentExecParams(const ExecutorConfig& executor_config,
      const TPlanExecInfo& plan_exec_info, FragmentScheduleState* fragment_state,
      ScheduleState* state);

  /// For a given 'src_state' and 'num_filters_per_host', select few backend as
  /// intermediate filter aggregator before final aggregation in coordinator.
  /// Do nothing if 'num_filters_per_host' <= 1.
  void ComputeRandomKrpcForAggregation(const ExecutorConfig& executor_config,
      ScheduleState* state, FragmentScheduleState* src_state, int num_filters_per_host);

  /// Create instances of the fragment corresponding to fragment_state, which contains
  /// either a Union node, one or more scan nodes, or both.
  ///
  /// This fragment is scheduled on the union of hosts of all scans in the fragment
  /// as well as the hosts of all its input fragments (s.t. a UnionNode with partitioned
  /// joins or grouping aggregates as children runs on at least as many hosts as the
  /// input to those children).
  ///
  /// The maximum number of instances per host is the value of query option mt_dop,
  /// or fragment's effective_instance_count if compute_processing_cost=true.
  /// For HDFS, this load balances among instances within a host using
  /// AssignRangesToInstances().
  void CreateCollocatedAndScanInstances(const ExecutorConfig& executor_config,
      FragmentScheduleState* fragment_state, ScheduleState* state);

  /// Does a round robin assignment of scan ranges 'ranges' that were assigned to a host
  /// to at most 'max_num_instances' fragment instances running on the same host.
  /// 'max_num_ranges' must be positive. Only returns non-empty vectors: if there are not
  /// enough ranges to create 'max_num_instances', fewer instances are assigned ranges.
  static std::vector<std::vector<ScanRangeParamsPB>> AssignRangesToInstances(
      int max_num_instances, std::vector<ScanRangeParamsPB>& ranges);

  /// For each instance of fragment_state's input fragment, create a collocated
  /// instance for fragment_state's fragment.
  /// Also enforces an upper limit on the number of instances in case this fragment_state
  /// has an HDFS table writer and the MAX_FS_WRITERS query option is non zero. This
  /// upper limit is enforced by doing a round robin assignment of instances among all the
  /// hosts that would run the input fragment.
  /// Expects that fragment_state only has a single input fragment.
  void CreateInputCollocatedInstances(
      FragmentScheduleState* fragment_state, ScheduleState* state);

  /// Create instances for a fragment that has a join build sink as its root.
  /// These instances will be collocated with the fragment instances that consume
  /// the join build. Therefore, those instances must have already been created
  /// by the scheduler.
  void CreateCollocatedJoinBuildInstances(
      FragmentScheduleState* fragment_state, ScheduleState* state);

  /// This is called during the execution of Schedule() and populates
  /// 'ScheduleState::query_schedule_pb_::by_node_filepath_to_hosts' mapping with the
  /// information of what files are scheduled to what hosts grouped by scan node.
  /// 'duplicate_check' keeps track of the previously added items and used for preventing
  /// duplicates being added.
  void PopulateFilepathToHostsMapping(const FInstanceScheduleState& finst,
      ScheduleState* state, ByNodeFilepathToHosts* duplicate_check);

  /// Add all hosts that the scans identified by 'scan_ids' are executed on to
  /// 'scan_hosts'.
  void GetScanHosts(const std::vector<TPlanNodeId>& scan_ids,
      const FragmentScheduleState& fragment_state,
      std::vector<NetworkAddressPB>* scan_hosts);

  /// Return true if 'plan' contains a node of the given type.
  static bool ContainsNode(const TPlan& plan, TPlanNodeType::type type);

  /// Return true if 'plan' contains a node of one of the given types.
  static bool ContainsNode(
      const TPlan& plan, const std::vector<TPlanNodeType::type>& types);

  /// Return true if 'plan' contains a scan node.
  static bool ContainsScanNode(const TPlan& plan);

  /// Return true if 'plan' contains a union node.
  static bool ContainsUnionNode(const TPlan& plan);

  /// Return all ids of nodes in 'plan' of any of the given types.
  std::vector<TPlanNodeId> FindNodes(
      const TPlan& plan, const std::vector<TPlanNodeType::type>& types);

  /// Return all ids of all scan nodes in 'plan'.
  std::vector<TPlanNodeId> FindScanNodes(const TPlan& plan);

  /// If TPlanFragment.effective_instance_count is positive, verify that resulting
  /// instance_states size match with effective_instance_count. Fragment with UnionNode or
  /// ScanNode or one where IsExceedMaxFsWriters equals true is not checked.
  static Status CheckEffectiveInstanceCount(
      const FragmentScheduleState* fragment_state, ScheduleState* state);

  /// Check if sink_fragment_state has hdfs_table_sink AND ref_fragment_state scheduled
  /// to exceed max_fs_writers query option.
  static inline bool IsExceedMaxFsWriters(
      const FragmentScheduleState* sink_fragment_state,
      const FragmentScheduleState* ref_fragment_state, const ScheduleState* state) {
    return (sink_fragment_state->fragment.output_sink.__isset.table_sink
        && sink_fragment_state->fragment.output_sink.table_sink.__isset.hdfs_table_sink
        && state->query_options().max_fs_writers > 0
        && ref_fragment_state->instance_states.size()
            > state->query_options().max_fs_writers);
  }

  friend class impala::test::SchedulerWrapper;
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentDeterministicNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomDiskLocal);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomRemote);
  FRIEND_TEST(SchedulerTest, TestMultipleFinstances);
};

}
