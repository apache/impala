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


#ifndef SCHEDULING_SIMPLE_SCHEDULER_H
#define SCHEDULING_SIMPLE_SCHEDULER_H

#include <gtest/gtest_prod.h>  // for FRIEND_TEST
#include <vector>
#include <string>
#include <list>
#include <boost/heap/binomial_heap.hpp>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "scheduling/scheduler.h"
#include "statestore/statestore-subscriber.h"
#include "statestore/statestore.h"
#include "util/metrics.h"
#include "util/runtime-profile.h"
#include "scheduling/admission-controller.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/ResourceBrokerService_types.h"
#include "rapidjson/rapidjson.h"

namespace impala {

class ResourceBroker;
class Coordinator;

class SchedulerWrapper;

/// Performs simple scheduling by matching between a list of backends configured
/// either from the statestore, or from a static list of addresses, and a list
/// of target data locations. The current set of backends is stored in backend_config_.
/// When receiving changes to the backend configuration from the statestore we will make a
/// copy of this configuration, apply the updates to the copy and atomically swap the
/// contents of the backend_config_ pointer.
///
/// TODO: Notice when there are duplicate statestore registrations (IMPALA-23)
/// TODO: Track assignments (assignment_ctx in ComputeScanRangeAssignment) per query
///       instead of per plan node?
/// TODO: Remove disable_cached_reads query option in CDH6
/// TODO: Replace the usage of shared_ptr with atomic_shared_ptr once compilers support
///       it. Alternatively consider using Kudu's rw locks.
/// TODO: Inject global dependencies into the class (for example ExecEnv::GetInstance(),
///       RNG used during scheduling, FLAGS_*)
///       to make it testable.
/// TODO: Benchmark the performance of the scheduler. The tests need to include setups
///       with:
///         - Small and large number of backends.
///         - Small and large query plans.
///         - Scheduling query plans with concurrent updates to the internal backend
///           configuration.
class SimpleScheduler : public Scheduler {
 public:
  static const std::string IMPALA_MEMBERSHIP_TOPIC;

  /// Initialize with a subscription manager that we can register with for updates to the
  /// set of available backends.
  ///  - backend_id - unique identifier for this Impala backend (usually a host:port)
  ///  - backend_address - the address that this backend listens on
  SimpleScheduler(StatestoreSubscriber* subscriber, const std::string& backend_id,
      const TNetworkAddress& backend_address, MetricGroup* metrics, Webserver* webserver,
      ResourceBroker* resource_broker, RequestPoolService* request_pool_service);

  /// Initialize with a list of <host:port> pairs in 'static' mode - i.e. the set of
  /// backends is fixed and will not be updated.
  SimpleScheduler(const std::vector<TNetworkAddress>& backends, MetricGroup* metrics,
      Webserver* webserver, ResourceBroker* resource_broker,
      RequestPoolService* request_pool_service);

  /// Register with the subscription manager if required
  virtual impala::Status Init();

  virtual Status Schedule(Coordinator* coord, QuerySchedule* schedule);
  virtual Status Release(QuerySchedule* schedule);
  virtual void HandlePreemptedReservation(const TUniqueId& reservation_id);
  virtual void HandlePreemptedResource(const TUniqueId& client_resource_id);
  virtual void HandleLostResource(const TUniqueId& client_resource_id);

 private:
  /// Type to store hostnames, which can be rfc1123 hostnames or IPv4 addresses.
  typedef std::string Hostname;

  /// Type to store IPv4 addresses.
  typedef std::string IpAddr;

  typedef std::list<TBackendDescriptor> BackendList;

  /// Map from a host's IP address to a list of backends running on that node.
  typedef boost::unordered_map<IpAddr, BackendList> BackendMap;

  /// Map from a host's IP address to the next backend to be round-robin scheduled for
  /// that host (needed for setups with multiple backends on a single host)
  typedef boost::unordered_map<IpAddr, BackendList::const_iterator> NextBackendPerHost;

  /// Map from a hostname to its IP address to support hostname based backend lookup.
  typedef boost::unordered_map<Hostname, IpAddr> BackendIpAddressMap;

  /// Configuration class to store a list of backends per IP address and a mapping from
  /// hostnames to IP addresses. backend_ip_map contains entries for all backends in
  /// backend_map and needs to be updated whenever backend_map changes. Each plan node
  /// creates a read-only copy of the scheduler's current backend_config_ to use during
  /// scheduling.
  class BackendConfig {
   public:
    BackendConfig() {}

    /// Construct config from list of backends.
    BackendConfig(const std::vector<TNetworkAddress>& backends);

    void AddBackend(const TBackendDescriptor& be_desc);
    void RemoveBackend(const TBackendDescriptor& be_desc);

    /// Look up the IP address of 'hostname' in the internal backend maps and return
    /// whether the lookup was successful. If 'hostname' itself is a valid IP address then
    /// it is copied to 'ip' and true is returned. 'ip' can be NULL if the caller only
    /// wants to check whether the lookup succeeds. Use this method to resolve datanode
    /// hostnames to IP addresses during scheduling, to prevent blocking on the OS.
    bool LookUpBackendIp(const Hostname& hostname, IpAddr* ip) const;

    int NumBackends() const { return backend_map().size(); }

    const BackendMap& backend_map() const { return backend_map_; }
    const BackendIpAddressMap& backend_ip_map() const { return backend_ip_map_; }

   private:
    BackendMap backend_map_;
    BackendIpAddressMap backend_ip_map_;
  };

  typedef std::shared_ptr<const BackendConfig> BackendConfigPtr;

  /// Internal structure to track scan range assignments for a backend host. This struct
  /// is used as the heap element in and maintained by AddressableAssignmentHeap.
  struct BackendAssignmentInfo {
    /// The number of bytes assigned to a backend host.
    int64_t assigned_bytes;

    /// Each host gets assigned a random rank to break ties in a random but deterministic
    /// order per plan node.
    const int random_rank;

    /// IP address of the backend.
    IpAddr ip;

    /// Compare two elements of this struct. The key is (assigned_bytes, random_rank).
    bool operator>(const BackendAssignmentInfo& rhs) const {
      if (assigned_bytes != rhs.assigned_bytes) {
        return assigned_bytes > rhs.assigned_bytes;
      }
      return random_rank > rhs.random_rank;
    }
  };

  /// Heap to compute candidates for scan range assignments. Elements are of type
  /// BackendAssignmentInfo and track assignment information for each backend. By default
  /// boost implements a max-heap so we use std::greater<T> to obtain a min-heap. This
  /// will make the top() element of the heap be the backend with the lowest number of
  /// assigned bytes and the lowest random rank.
  typedef boost::heap::binomial_heap<BackendAssignmentInfo,
      boost::heap::compare<std::greater<BackendAssignmentInfo>>> AssignmentHeap;

  /// Map to look up handles to heap elements to modify heap element keys.
  typedef boost::unordered_map<IpAddr, AssignmentHeap::handle_type> BackendHandleMap;

  /// Class to store backend information in an addressable heap. In addition to
  /// AssignmentHeap it can be used to look up heap elements by their IP address and
  /// update their key. For each plan node we create a new heap, so they are not shared
  /// between concurrent invocations of the scheduler.
  class AddressableAssignmentHeap {
   public:
    const AssignmentHeap& backend_heap() const { return backend_heap_; }
    const BackendHandleMap& backend_handles() const { return backend_handles_; }

    void InsertOrUpdate(const IpAddr& ip, int64_t assigned_bytes, int rank);

    // Forward interface for boost::heap
    decltype(auto) size() const { return backend_heap_.size(); }
    decltype(auto) top() const { return backend_heap_.top(); }

    // Forward interface for boost::unordered_map
    decltype(auto) find(const IpAddr& ip) const { return backend_handles_.find(ip); }
    decltype(auto) end() const { return backend_handles_.end(); }

   private:
    // Heap to determine next backend.
    AssignmentHeap backend_heap_;
    // Maps backend IPs to handles in the heap.
    BackendHandleMap backend_handles_;
  };

  /// Class to store context information on assignments during scheduling. It is
  /// initialized with a copy of the global backend information and assigns a random rank
  /// to each backend to break ties in cases where multiple backends have been assigned
  /// the same number or bytes. It tracks the number of assigned bytes, which backends
  /// have already been used, etc. Objects of this class are created in
  /// ComputeScanRangeAssignment() and thus don't need to be thread safe.
  class AssignmentCtx {
   public:
    AssignmentCtx(const BackendConfig& backend_config, IntCounter* total_assignments,
        IntCounter* total_local_assignments);

    /// Among hosts in 'data_locations', select the one with the minimum number of
    /// assigned bytes. If backends have been assigned equal amounts of work and
    /// 'break_ties_by_rank' is true, then the backend rank is used to break ties.
    /// Otherwise the first backend according to their order in 'data_locations' is
    /// selected.
    const IpAddr* SelectLocalBackendHost(const std::vector<IpAddr>& data_locations,
        bool break_ties_by_rank);

    /// Select a backend host for a remote read. If there are unused backend hosts, then
    /// those will be preferred. Otherwise the one with the lowest number of assigned
    /// bytes is picked. If backends have been assigned equal amounts of work, then the
    /// backend rank is used to break ties.
    const IpAddr* SelectRemoteBackendHost();

    /// Return the next backend that has not been assigned to. This assumes that a
    /// returned backend will also be assigned to. The caller must make sure that
    /// HasUnusedBackends() is true.
    const IpAddr* GetNextUnusedBackendAndIncrement();

    /// Pick a backend in round-robin fashion from multiple backends on a single host.
    void SelectBackendOnHost(const IpAddr& backend_ip, TBackendDescriptor* backend);

    /// Build a new TScanRangeParams object and append it to the assignment list for the
    /// tuple (backend, node_id) in 'assignment'. Also, update assignment_heap_ and
    /// assignment_byte_counters_, increase the counters 'total_assignments_' and
    /// 'total_local_assignments_'. 'scan_range_locations' contains information about the
    /// scan range and its replica locations.
    void RecordScanRangeAssignment(const TBackendDescriptor& backend, PlanNodeId node_id,
        const vector<TNetworkAddress>& host_list,
        const TScanRangeLocations& scan_range_locations,
        FragmentScanRangeAssignment* assignment);

    const BackendConfig& backend_config() const { return backend_config_; }
    const BackendMap& backend_map() const { return backend_config_.backend_map(); }

    /// Print the assignment and statistics to VLOG_FILE.
    void PrintAssignment(const FragmentScanRangeAssignment& assignment);

   private:
    /// A struct to track various counts of assigned bytes during scheduling.
    struct AssignmentByteCounters {
      int64_t remote_bytes = 0;
      int64_t local_bytes = 0;
      int64_t cached_bytes = 0;
    };

    /// Used to look up hostnames to IP addresses and IP addresses to backend.
    const BackendConfig& backend_config_;

    // Addressable heap to select remote backends from. Elements are ordered by the number
    // of already assigned bytes (and a random rank to break ties).
    AddressableAssignmentHeap assignment_heap_;

    /// Store a random rank per backend host to break ties between otherwise equivalent
    /// replicas (e.g., those having the same number of assigned bytes).
    boost::unordered_map<IpAddr, int> random_backend_rank_;

    // Index into random_backend_order. It points to the first unused backend and is used
    // to select unused backends and inserting them into the assignment_heap_.
    int first_unused_backend_idx_;

    /// Store a random permutation of backend hosts to select backends from.
    std::vector<const BackendMap::value_type*> random_backend_order_;

    /// Track round robin information per backend host.
    NextBackendPerHost next_backend_per_host_;

    /// Track number of assigned bytes that have been read from cache, locally, or
    /// remotely.
    AssignmentByteCounters assignment_byte_counters_;

    /// Pointers to the scheduler's counters.
    IntCounter* total_assignments_;
    IntCounter* total_local_assignments_;

    /// Return whether there are backends that have not been assigned a scan range.
    bool HasUnusedBackends() const;

    /// Return the rank of a backend.
    int GetBackendRank(const IpAddr& ip) const;
  };

  /// The scheduler's backend configuration. When receiving changes to the backend
  /// configuration from the statestore we will make a copy of the stored object, apply
  /// the updates to the copy and atomically swap the contents of this pointer.
  BackendConfigPtr backend_config_;

  /// Protect access to backend_config_ which might otherwise be updated asynchronously
  /// with respect to reads.
  mutable boost::mutex backend_config_lock_;

  /// Total number of scan ranges assigned to backends during the lifetime of the
  /// scheduler.
  int64_t num_assignments_;

  /// Map from unique backend id to TBackendDescriptor. Used to track the known backends
  /// from the statestore. It's important to track both the backend ID as well as the
  /// TBackendDescriptor so we know what is being removed in a given update.
  /// Locking of this map is not needed since it should only be read/modified from
  /// within the UpdateMembership() function.
  typedef boost::unordered_map<std::string, TBackendDescriptor> BackendIdMap;
  BackendIdMap current_membership_;

  /// MetricGroup subsystem access
  MetricGroup* metrics_;

  /// Webserver for /backends. Not owned by us.
  Webserver* webserver_;

  /// Pointer to a subscription manager (which we do not own) which is used to register
  /// for dynamic updates to the set of available backends. May be NULL if the set of
  /// backends is fixed.
  StatestoreSubscriber* statestore_subscriber_;

  /// Unique - across the cluster - identifier for this impala backend.
  const std::string local_backend_id_;

  /// Describe this backend, including the Impalad service address.
  TBackendDescriptor local_backend_descriptor_;

  ThriftSerializer thrift_serializer_;

  /// Locality metrics
  IntCounter* total_assignments_;
  IntCounter* total_local_assignments_;

  /// Initialization metric
  BooleanProperty* initialized_;

  /// Current number of backends
  IntGauge* num_fragment_instances_metric_;

  /// Protect active_reservations_ and active_client_resources_.
  boost::mutex active_resources_lock_;

  /// Map from a Llama reservation id to the coordinator of the query using that
  /// reservation. The map is used to cancel queries whose reservation has been preempted.
  /// Entries are added in Schedule() calls that result in granted resource allocations.
  /// Entries are removed in Release().
  typedef boost::unordered_map<TUniqueId, Coordinator*> ActiveReservationsMap;
  ActiveReservationsMap active_reservations_;

  /// Map from client resource id to the coordinator of the query using that resource.
  /// The map is used to cancel queries whose resource(s) have been preempted.
  /// Entries are added in Schedule() calls that result in granted resource allocations.
  /// Entries are removed in Release().
  typedef boost::unordered_map<TUniqueId, Coordinator*> ActiveClientResourcesMap;
  ActiveClientResourcesMap active_client_resources_;

  /// Resource broker that mediates resource requests between Impala and the Llama.
  /// Set to NULL if resource management is disabled.
  ResourceBroker* resource_broker_;

  /// Used for user-to-pool resolution and looking up pool configurations. Not owned by
  /// us.
  RequestPoolService* request_pool_service_;

  /// Used to make admission decisions in 'Schedule()'
  boost::scoped_ptr<AdmissionController> admission_controller_;

  /// Helper methods to access backend_config_ (the shared_ptr, not its contents),
  /// protecting the access with backend_config_lock_.
  BackendConfigPtr GetBackendConfig() const;
  void SetBackendConfig(const BackendConfigPtr& backend_config);

  /// Return a list of all backends registered with the scheduler.
  void GetAllKnownBackends(BackendList* backends);

  /// Add the granted reservation and resources to the active_reservations_ and
  /// active_client_resources_ maps, respectively.
  void AddToActiveResourceMaps(
      const TResourceBrokerReservationResponse& reservation, Coordinator* coord);

  /// Remove the given reservation and resources from the active_reservations_ and
  /// active_client_resources_ maps, respectively.
  void RemoveFromActiveResourceMaps(
      const TResourceBrokerReservationResponse& reservation);

  /// Called asynchronously when an update is received from the subscription manager
  void UpdateMembership(const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Webserver callback that produces a list of known backends.
  /// Example output:
  /// "backends": [
  ///     "henry-metrics-pkg-cdh5.ent.cloudera.com:22000"
  ///              ],
  void BackendsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Determine the pool for a user and query options via request_pool_service_.
  Status GetRequestPool(const std::string& user, const TQueryOptions& query_options,
      std::string* pool) const;

  /// Compute the assignment of scan ranges to hosts for each scan node in 'schedule'.
  /// Unpartitioned fragments are assigned to the coordinator. Populate the schedule's
  /// fragment_exec_params_ with the resulting scan range assignment.
  Status ComputeScanRangeAssignment(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// Process the list of scan ranges of a single plan node and compute scan range
  /// assignments (returned in 'assignment'). The result is a mapping from hosts to their
  /// assigned scan ranges per plan node.
  ///
  /// If exec_at_coord is true, all scan ranges will be assigned to the coordinator host.
  /// Otherwise the assignment is computed for each scan range as follows:
  ///
  /// Scan ranges refer to data, which is usually replicated on multiple hosts. All scan
  /// ranges where one of the replica hosts also runs an impala backend are processed
  /// first. If more than one of the replicas run an impala backend, then the 'memory
  /// distance' of each backend is considered. The concept of memory distance reflects the
  /// cost of moving data into the processing backend's main memory. Reading from cached
  /// replicas is generally considered less costly than reading from a local disk, which
  /// in turn is cheaper than reading data from a remote node. If multiple backends of the
  /// same memory distance are found, then the one with the least amount of previously
  /// assigned work is picked, thus aiming to distribute the work as evenly as possible.
  ///
  /// Finally, scan ranges are considered which do not have an impalad backend running on
  /// any of their data nodes. They will be load-balanced by assigned bytes across all
  /// backends
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
  /// disable_cached_reads:
  ///   Setting this value to true is equivalent to setting replica_preference to
  ///   DISK_LOCAL and takes precedence over replica_preference. The default setting is
  ///   false.
  ///
  /// schedule_random_replica:
  ///   When equivalent backends with a memory distance of DISK_LOCAL are found for a scan
  ///   range (same memory distance, same amount of assigned work), then the first one
  ///   will be picked deterministically. This aims to make better use of OS buffer
  ///   caches, but can lead to performance bottlenecks on individual hosts. Setting this
  ///   option to true will randomly change the order in which equivalent replicas are
  ///   picked for different plan nodes. This helps to compute a more even assignment,
  ///   with the downside being an increased memory usage for OS buffer caches. The
  ///   default setting is false. Selection between equivalent replicas with memory
  ///   distance of CACHE_LOCAL or REMOTE happens based on a random order.
  ///
  /// The method takes the following parameters:
  ///
  /// backend_config:          Backend configuration to use for scheduling.
  /// node_id:                 ID of the plan node.
  /// node_replica_preference: Query hint equivalent to replica_preference.
  /// node_random_replica:     Query hint equivalent to schedule_random_replica.
  /// locations:               List of scan ranges to be assigned to backends.
  /// host_list:               List of hosts, into which 'locations' will index.
  /// exec_at_coord:           Whether to schedule all scan ranges on the coordinator.
  /// query_options:           Query options for the current query.
  /// timer:                   Tracks execution time of ComputeScanRangeAssignment.
  /// assignment:              Output parameter, to which new assignments will be added.
  Status ComputeScanRangeAssignment(const BackendConfig& backend_config,
      PlanNodeId node_id, const TReplicaPreference::type* node_replica_preference,
      bool node_random_replica, const std::vector<TScanRangeLocations>& locations,
      const std::vector<TNetworkAddress>& host_list, bool exec_at_coord,
      const TQueryOptions& query_options, RuntimeProfile::Counter* timer,
      FragmentScanRangeAssignment* assignment);

  /// Populate fragment_exec_params_ in schedule.
  void ComputeFragmentExecParams(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// For each fragment in exec_request, compute the hosts on which to run the instances
  /// and stores result in fragment_exec_params_.hosts.
  void ComputeFragmentHosts(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// Return the id of the leftmost node of any of the given types in 'plan', or
  /// INVALID_PLAN_NODE_ID if no such node present.
  PlanNodeId FindLeftmostNode(
      const TPlan& plan, const std::vector<TPlanNodeType::type>& types);

  /// Return the index (w/in exec_request.fragments) of fragment that sends its output to
  /// exec_request.fragment[fragment_idx]'s leftmost ExchangeNode.
  /// Return INVALID_PLAN_NODE_ID if the leftmost node is not an exchange node.
  int FindLeftmostInputFragment(int fragment_idx, const TQueryExecRequest& exec_request);

  /// Add all hosts the given scan is executed on to scan_hosts.
  void GetScanHosts(TPlanNodeId scan_id, const TQueryExecRequest& exec_request,
      const FragmentExecParams& params, std::vector<TNetworkAddress>* scan_hosts);

  /// Return true if 'plan' contains a node of the given type.
  bool ContainsNode(const TPlan& plan, TPlanNodeType::type type);

  /// Return all ids of nodes in 'plan' of any of the given types.
  void FindNodes(const TPlan& plan, const std::vector<TPlanNodeType::type>& types,
      std::vector<TPlanNodeId>* results);

  /// Returns the index (w/in exec_request.fragments) of fragment that sends its output
  /// to the given exchange in the given fragment index.
  int FindSenderFragment(TPlanNodeId exch_id, int fragment_idx,
      const TQueryExecRequest& exec_request);

  /// Deterministically resolve a host to one of its IP addresses. This method will call
  /// into the OS, so it can take a long time to return. Use this method to resolve
  /// hostnames during initialization and while processing statestore updates.
  static Status HostnameToIpAddr(const Hostname& hostname, IpAddr* ip);

  friend class impala::SchedulerWrapper;
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentDeterministicNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomNonCached);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomDiskLocal);
  FRIEND_TEST(SimpleAssignmentTest, ComputeAssignmentRandomRemote);
};

}

#endif
