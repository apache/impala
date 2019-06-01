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

#ifndef SCHEDULING_SCHEDULER_H
#define SCHEDULING_SCHEDULER_H

#include <list>
#include <string>
#include <vector>
#include <boost/heap/binomial_heap.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_generated.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rapidjson/document.h"
#include "rpc/thrift-util.h"
#include "scheduling/executor-group.h"
#include "scheduling/query-schedule.h"
#include "scheduling/request-pool-service.h"
#include "statestore/statestore-subscriber.h"
#include "util/metrics-fwd.h"
#include "util/network-util.h"
#include "util/runtime-profile.h"

namespace impala {
class ClusterMembershipMgr;

namespace test {
class SchedulerWrapper;
}

/// Performs simple scheduling by matching between a list of executor backends that it
/// retrieves from the cluster membership manager, and a list of target data locations.
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
  Scheduler(ClusterMembershipMgr* cluster_membership_mgr, MetricGroup* metrics,
      RequestPoolService* request_pool_service);

  /// Populates given query schedule and assigns fragments to hosts based on scan
  /// ranges in the query exec request.
  Status Schedule(QuerySchedule* schedule);

 private:
  /// Current snapshot of executors to be used for scheduling a scan.
  struct ExecutorConfig {
    const ExecutorGroup& group;
    const TBackendDescriptor& local_be_desc;
  };

  /// Map from a host's IP address to the next executor to be round-robin scheduled for
  /// that host (needed for setups with multiple executors on a single host)
  typedef boost::unordered_map<IpAddr, ExecutorGroup::Executors::const_iterator>
      NextExecutorPerHost;

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
        IntCounter* total_local_assignments);

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
    void SelectExecutorOnHost(const IpAddr& executor_ip, TBackendDescriptor* executor);

    /// Build a new TScanRangeParams object and append it to the assignment list for the
    /// tuple (executor, node_id) in 'assignment'. Also, update assignment_heap_ and
    /// assignment_byte_counters_, increase the counters 'total_assignments_' and
    /// 'total_local_assignments_'. 'scan_range_locations' contains information about the
    /// scan range and its replica locations.
    void RecordScanRangeAssignment(const TBackendDescriptor& executor, PlanNodeId node_id,
        const vector<TNetworkAddress>& host_list,
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

  /// Pointer to the cluster membership manager. It provides information about backends
  /// and executors in the cluster that the scheduler uses to assign fragment instances to
  /// backends.
  ClusterMembershipMgr* cluster_membership_mgr_;

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
  const TBackendDescriptor& LookUpBackendDesc(
      const ExecutorConfig& executor_config, const TNetworkAddress& host);

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
  Status ComputeScanRangeAssignment(const ExecutorConfig& executor_config,
      QuerySchedule* schedule);

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
  /// assignment:              Output parameter, to which new assignments will be added.
  Status ComputeScanRangeAssignment(const ExecutorConfig& executor_config,
      PlanNodeId node_id, const TReplicaPreference::type* node_replica_preference,
      bool node_random_replica, const std::vector<TScanRangeLocationList>& locations,
      const std::vector<TNetworkAddress>& host_list, bool exec_at_coord,
      const TQueryOptions& query_options, RuntimeProfile::Counter* timer,
      FragmentScanRangeAssignment* assignment);

  /// Computes BackendExecParams for all backends assigned in the query. Must be called
  /// after ComputeFragmentExecParams().
  void ComputeBackendExecParams(
      const ExecutorConfig& executor_config, QuerySchedule* schedule);

  /// Compute the FragmentExecParams for all plans in the schedule's
  /// TQueryExecRequest.plan_exec_info.
  /// This includes the routing information (destinations, per_exch_num_senders,
  /// sender_id)
  /// 'executor_config' is the executor configuration to use for scheduling.
  void ComputeFragmentExecParams(const ExecutorConfig& executor_config,
      QuerySchedule* schedule);

  /// Recursively create FInstanceExecParams and set per_node_scan_ranges for
  /// fragment_params and its input fragments via a depth-first traversal.
  /// All fragments are part of plan_exec_info.
  void ComputeFragmentExecParams(const ExecutorConfig& executor_config,
      const TPlanExecInfo& plan_exec_info, FragmentExecParams* fragment_params,
      QuerySchedule* schedule);

  /// Create instances of the fragment corresponding to fragment_params, which contains
  /// a Union node.
  /// UnionNodes are special because they can consume multiple partitioned inputs,
  /// as well as execute multiple scans in the same fragment.
  /// Fragments containing a UnionNode are executed on the union of hosts of all
  /// scans in the fragment as well as the hosts of all its input fragments (s.t.
  /// a UnionNode with partitioned joins or grouping aggregates as children runs on
  /// at least as many hosts as the input to those children).
  /// TODO: is this really necessary? If not, revise.
  void CreateUnionInstances(const ExecutorConfig& executor_config,
      FragmentExecParams* fragment_params, QuerySchedule* schedule);

  /// Create instances of the fragment corresponding to fragment_params to run on the
  /// selected replica hosts of the scan ranges of the node with id scan_id.
  /// The maximum number of instances is the value of query option mt_dop.
  /// For HDFS, this attempts to load balance among instances by computing the average
  /// number of bytes per instances and then in a single pass assigning scan ranges to
  /// each instance to roughly meet that average.
  /// For all other storage mgrs, it load-balances the number of splits per instance.
  void CreateScanInstances(const ExecutorConfig& executor_config, PlanNodeId scan_id,
      FragmentExecParams* fragment_params, QuerySchedule* schedule);

  /// For each instance of fragment_params's input fragment, create a collocated
  /// instance for fragment_params's fragment.
  /// Expects that fragment_params only has a single input fragment.
  void CreateCollocatedInstances(
      FragmentExecParams* fragment_params, QuerySchedule* schedule);

  /// Return the id of the leftmost node of any of the given types in 'plan', or
  /// INVALID_PLAN_NODE_ID if no such node present.
  PlanNodeId FindLeftmostNode(
      const TPlan& plan, const std::vector<TPlanNodeType::type>& types);
  /// Same for scan nodes.
  PlanNodeId FindLeftmostScan(const TPlan& plan);

  /// Add all hosts the given scan is executed on to scan_hosts.
  void GetScanHosts(const TBackendDescriptor& local_be_desc, TPlanNodeId scan_id,
      const FragmentExecParams& params, std::vector<TNetworkAddress>* scan_hosts);

  /// Return true if 'plan' contains a node of the given type.
  bool ContainsNode(const TPlan& plan, TPlanNodeType::type type);

  /// Return all ids of nodes in 'plan' of any of the given types.
  void FindNodes(const TPlan& plan, const std::vector<TPlanNodeType::type>& types,
      std::vector<TPlanNodeId>* results);

  friend class impala::test::SchedulerWrapper;
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentDeterministicNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomDiskLocal);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomRemote);
};

}

#endif
