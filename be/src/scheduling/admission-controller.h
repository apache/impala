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

#ifndef SCHEDULING_ADMISSION_CONTROLLER_H
#define SCHEDULING_ADMISSION_CONTROLLER_H

#include <list>
#include <string>
#include <utility>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <gtest/gtest_prod.h>

#include "common/status.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/query-schedule.h"
#include "statestore/statestore-subscriber.h"
#include "util/condition-variable.h"
#include "util/internal-queue.h"
#include "util/runtime-profile.h"
#include "util/thread.h"

namespace impala {

class ExecEnv;

/// Represents the admission outcome of a query. It is stored in the 'admit_outcome'
/// input variable passed to AdmissionController::AdmitQuery() if an admission decision
/// has been made or the caller has initiated a cancellation.
enum class AdmissionOutcome {
  ADMITTED,
  REJECTED,
  TIMED_OUT,
  CANCELLED,
};

/// The AdmissionController is used to throttle requests (e.g. queries, DML) based
/// on available cluster resources, which are configured in one or more resource pools. A
/// request will either be admitted for immediate execution, queued for later execution,
/// or rejected (either immediately or after being queued). Resource pools can be
/// configured to have maximum number of concurrent queries, maximum cluster wide memory,
/// maximum queue size, max and min per host memory limit for every query, and to set
/// whether the mem_limit query option will be clamped by the previously mentioned max/min
/// per host limits or not. Queries will be queued if there are already too many queries
/// executing or there isn't enough available memory. Once the queue reaches the maximum
/// queue size, incoming queries will be rejected. Requests in the queue will time out
/// after a configurable timeout.
///
/// Depending on the -is_coordinator startup flag, multiple impalads can act as a
/// coordinator and thus also an admission controller, so some cluster state must be
/// shared between impalads in order to make admission decisions on any of them. Every
/// coordinator maintains some per-pool and per-host statistics related to the requests it
/// itself is servicing as the admission controller. Some of these local admission
/// statistics in addition to some backend-specific statistics (i.e. the backend executor
/// associated with the same impalad process) are disseminated across the cluster via the
/// statestore using the IMPALA_REQUEST_QUEUE_TOPIC topic. Effectively, coordinators send
/// statestore updates where the admission statistics reflect the load and all
/// participating backends send statestore updates reflecting the load they're executing.
///
/// Every <impalad, pool> pair is sent as a topic update at the statestore heartbeat
/// interval when pool statistics change, and the topic updates from other impalads are
/// used to re-compute the aggregate per-pool stats. Because the pool statistics are only
/// updated on statestore heartbeats and all decisions are made with the cached state,
/// the aggregate pool statistics are only estimates. As a result, more requests may be
/// admitted or queued than the configured thresholds, which are really soft limits.
///
/// Memory resources:
/// A pool may be configured to allow a maximum amount of memory resources to be
/// 'reserved' by requests admitted to that pool. While Impala does not yet truly
/// 'reserve' the memory at admission (i.e. Impala does not yet guarantee the memory for
/// a request, it is still possible to overadmit such that multiple queries think they
/// have reserved the same memory), the admission controller uses several metrics to
/// estimate the available memory and admit only when it thinks the necessary memory is
/// available. Future work will enable real reservations, but this is a much larger
/// effort and will involve changes outside of the admission controller.
///
/// The memory required for admission for a request is specified as the query option
/// MEM_LIMIT (either explicitly or via a default value). This is a per-host value. If
/// there is no memory limit, the per-host estimate from planning is used instead as a
/// memory limit and a lower bound is enforced on it based on the largest initial
/// reservation of the query. The final memory limit used is also clamped by the max/min
/// memory limits configured for the pool with an option to not enforce these limits on
/// the MEM_LIMIT query option (If both these max/min limits are not configured, then the
/// estimates from planning are not used as a memory limit and are only used for making
/// admission decisions. Moreover the estimates will no longer have a lower bound based on
/// the largest initial reservation).
/// The following four conditions must hold in order for the request to be admitted:
///  1) The current pool configuration is valid.
///  2) There must be enough memory resources available in this resource pool for the
///     request. The max memory resources configured for the resource pool specifies the
///     aggregate, cluster-wide memory that may be reserved by all executing queries in
///     this pool. Thus the aggregate memory to be reserved across all participating
///     backends for this request, *plus* that of already admitted requests must be less
///     than or equal to the max resources specified.
///  3) All participating backends must have enough memory available. Each impalad has a
///     per-process mem limit, and that is the max memory that can be reserved on that
///     backend.
///  3b) (optional) When using executor groups (see below) and admitting to the
///     non-default executor group, then the number of currently running queries must be
///     below the configured maximum for all participating backends.
///  4) The final per host memory limit used can accommodate the largest initial
///     reservation.
///
/// In order to admit based on these conditions, the admission controller accounts for
/// the following on both a per-host and per-pool basis:
///  a) Mem Reserved: the amount of memory that has been reported as reserved by all
///     backends, which come from the statestore topic updates. The values that are sent
///     come from the pool mem trackers in UpdateMemTrackerStats(), which reflects the
///     memory reserved by fragments that have begun execution. For queries that are
///     executing and have mem limits, the limit is considered to be its reserved memory
///     because it may consume up to that limit. Otherwise the query's current consumption
///     is used (see MemTracker::GetPoolMemReserved()). The per-pool and per-host
///     aggregates are computed in UpdateClusterAggregates(). This state, once all updates
///     are fully distributed and aggregated, provides enough information to make
///     admission decisions by any impalad. However, this requires waiting for both
///     admitted requests to start all remote fragments and then for the updated state to
///     be distributed via the statestore.
///  b) Mem Admitted: the amount of memory required (i.e. the value used in admission,
///     either the mem limit or estimate) for the requests that this impalad's admission
///     controller has admitted. Both the per-pool and per-host accounting is updated
///     when requests are admitted and released (and NOTE: not via the statestore, so
///     there is no latency, but this does not account for memory from requests admitted
///     by other impalads).
///  c) Num Admitted: the number of queries that have been admitted and are therefore
///     considered to be currently running. Note that there is currently no equivalent to
///     the reserved memory reporting, i.e. hosts do not report the actual number of
///     queries that are currently executing (IMPALA-8762). This prevents using multiple
///     coordinators with executor groups.
///
/// As described, both the 'reserved' and 'admitted' mem accounting mechanisms have
/// different advantages and disadvantages. The 'reserved' mem accounting works well in
/// the steady state, i.e. given enough time to distribute updates. The 'admitted'
/// mem accounting works perfectly when there is a single coordinator (and perhaps works
/// reasonably with just a few). The maximum of the reserved and admitted mem is used in
/// making admission decisions, which works well when either relatively few coordinators
/// are used or, if there is a wide distribution of requests across impalads, the rate of
/// submission is low enough that new state is able to be updated by the statestore.
///
/// Releasing Queries:
/// When queries complete they must be explicitly released from the admission controller
/// using the methods 'ReleaseQuery' and 'ReleaseQueryBackends'. These methods release
/// the admitted memory and decrement the number of admitted queries for the resource
/// pool. All Backends for a query must be released via 'ReleaseQueryBackends' before the
/// query is released using 'ReleaseQuery'. Releasing Backends releases the admitted
/// memory used by that Backend and decrements the number of running queries on the host
/// running that Backend. Releasing a query does not release any admitted memory, it only
/// decrements the number of running queries in the resource pool.
///
/// Executor Groups:
/// Executors in a cluster can be assigned to executor groups. Each executor can only be
/// in one group. A resource pool can have multiple executor groups associated with it.
/// Each executor group belongs to a single resource pool and will only serve requests
/// from that pool. I.e. the relationships are 1 resource pool : many executor groups and
/// 1 executor group : many executors.



///
/// Executors that don't specify an executor group name during startup are automatically
/// added to a default group called DEFAULT_EXECUTOR_GROUP_NAME. The default executor
/// group does not enforce query concurrency limits per host and as such can be admitted
/// to by multiple coordinators.
///
/// Executor groups are mapped to resource pools implicitly by their name. Queries in a
/// resource pool can run on all executor groups whose name starts with the pool's name,
/// separated by a '-'. For example, queries in a pool with name 'q1' can run on all
/// executor groups starting with 'q1-'. If no matching executor groups can be found for a
/// resource pool and the default executor group is not empty, then the default group is
/// used.
///
/// In addition to the checks described before, admission to executor groups is bounded by
/// the maximum number of queries that can run concurrently on an executor
/// (-max_concurrent_queries). An additional check is performed to ensure that each
/// executor in the group has an available slot to run the query. Admission controllers
/// include the number of queries that have been admitted to each executor in the
/// statestore updates.
///
/// In order to find an executor group that can run a query, the admission controller
/// calls FindGroupToAdmitOrReject(), either during the initial admission attempt or in
/// DequeueLoop(). If the cluster membership has changed, it (re-)computes schedules for
/// all executor groups and then tries to admit queries using the list of schedules.
/// Admission is always attempted in the same order so that executor groups fill up before
/// further ones are considered. In particular, we don't attempt to balance the queries
/// across executor groups.
///
/// Example without executor groups:
/// Consider a 10-node cluster with 100gb/node and a resource pool 'q1' configured with
/// 500gb of aggregate memory and 40gb as the max memory limit. An incoming request with
/// the MEM_LIMIT query option set to 50gb and scheduled to execute on all backends is
/// received by SubmitForAdmission() on an otherwise quiet cluster. Based on the pool
/// configuration, a per host mem limit of 40gb is used for this query and for any
/// subsequent checks that it needs to pass prior to admission. FindGroupToAdmitOrReject()
/// computes a schedule for the default executor group and performs rejection tests before
/// calling CanAdmitRequest(), which checks the number of running queries and then calls
/// HasAvailableMemResources() to check for memory resources. It first checks whether
/// there is enough memory for the request using PoolStats::EffectiveMemReserved() (which
/// is the max of the pool's agg_mem_reserved_ and local_mem_admitted_, see #1 above),
/// then checks for enough memory on each individual host via the max of mem_reserved and
/// mem_admitted in hosts_stats_ (see #2 above) and finally checks if the memory limit
/// used for this query can accommodate its largest initial reservation. In this case,
/// ample resources are available so CanAdmitRequest() returns true. PoolStats::Admit() is
/// called to update q1's PoolStats: it first updates agg_num_running_ and
/// local_mem_admitted_ which are available to be used immediately for incoming admission
/// requests, then it updates num_admitted_running in the struct sent to the statestore
/// (local_stats_). UpdateHostStats() is called to update the per-host admitted mem
/// (stored in the map host_stats_) for all participating hosts. Then SubmitForAdmission()
/// returns to the ClientRequestState. If another identical admission request is received
/// by the same coordinator immediately, it will be rejected because q1's
/// local_mem_admitted_ is already 400gb. If that request were sent to another impalad at
/// the same time, it would have been admitted because not all updates have been
/// disseminated yet. The next statestore update will contain the updated value of
/// num_admitted_running for q1 on this backend. As remote fragments begin execution on
/// remote impalads, their pool mem trackers will reflect the updated amount of memory
/// reserved (set in local_stats_.backend_mem_reserved by UpdateMemTrackerStats()) and the
/// next statestore updates coming from those impalads will contain the updated value. As
/// the statestore updates are received (in the subscriber callback fn UpdatePoolStats()),
/// the incoming per-backend, per-pool mem_reserved values are aggregated to
/// PoolStats::agg_mem_reserved_ (pool aggregate over all hosts) and backend_mem_reserved_
/// (per-host aggregates over all pools). Once this has happened, any incoming admission
/// request now has the updated state required to make correct admission decisions.
///
/// Example with executor groups:
/// Consider a cluster with a dedicated coordinator and 2 executor groups
/// "default-pool-group-1" and "default-pool-group-2" (the number of executors per group
/// does not matter for this example). Both executor groups will be able to serve requests
/// from the default resource pool. Consider that each executor can only run one query at
/// a time, i.e. --max_concurrent_queries=1 is specified for all executors. An incoming
/// query is submitted through SubmitForAdmission(), which calls
/// FindGroupToAdmitOrReject(). From there we call ComputeGroupSchedules() which calls
/// compute schedules for both executor groups. Then we perform rejection tests and
/// afterwards call CanAdmitRequest() for each of the schedules. Executor groups are
/// processed in alphanumerically sorted order, so we attempt admission to group
/// "default-pool-group-1" first. CanAdmitRequest() calls HasAvailableSlot() to check
/// whether any of the hosts in the group have reached their maximum number of concurrent
/// queries and since that is not the case, admission succeeds. The query is admitted and
/// 'num_admitted' is incremented for each host in that group. When a second query arrives
/// while the first one is still running, we perform the same steps. In particular we
/// compute schedules for both groups and consider admission to default-pool-group-1
/// first. However, the check in HasAvailableSlot() now fails and we will consider group
/// default-pool-group-2 next. For this group, the check succeeds and the query is
/// admitted, incrementing the num_admitted counter for each host in group
/// default-pool-group-2.
///
/// Queuing Behavior:
/// Once the resources in a pool are consumed, each coordinator receiving requests will
/// begin queuing. While each individual queue is FIFO, there is no total ordering on the
/// queued requests between admission controllers and no FIFO behavior is guaranteed for
/// requests submitted to different coordinators. When resources become available, there
/// is no synchronous coordination between nodes used to determine which get to dequeue
/// and admit requests. Instead, we use a simple heuristic to try to dequeue a number of
/// requests proportional to the number of requests that are waiting in each individual
/// admission controller to the total number of requests queued across all admission
/// controllers (i.e. impalads). This limits the amount of overadmission that may result
/// from a large amount of resources becoming available at the same time. When there are
/// requests queued in multiple pools on the same host, the admission controller simply
/// iterates over the pools in pool_stats_ and attempts to dequeue from each. This is fine
/// for the max_requests limit, but is unfair for memory-based admission because the
/// iteration order of pools effectively gives priority to the queues at the beginning.
/// Requests across queues may be competing for the same resources on particular hosts,
/// i.e. #2 in the description of memory-based admission above. Note the pool's
/// max_mem_resources (#1) is not contented.
///
/// Cancellation Behavior:
/// An admission request<schedule, admit_outcome> submitted using SubmitForAdmission() can
/// be proactively cancelled by setting the 'admit_outcome' to
/// AdmissionOutcome::CANCELLED. This is handled asynchronously by SubmitForAdmission()
/// and DequeueLoop().
///
/// Pool Configuration Mechanism:
/// The path to pool config files are specified using the startup flags
/// "fair_scheduler_allocation_path" and "llama_site_path". The format for specifying pool
/// configs is based on yarn and llama with additions specific to Impala. A file
/// monitoring service is started that monitors changes made to these files. Those changes
/// are only propagated to Impala when a new query is serviced. See RequestPoolService
/// class for more details.
///

class AdmissionController {
 public:
  // Profile info strings
  static const std::string PROFILE_INFO_KEY_ADMISSION_RESULT;
  static const std::string PROFILE_INFO_VAL_ADMIT_IMMEDIATELY;
  static const std::string PROFILE_INFO_VAL_QUEUED;
  static const std::string PROFILE_INFO_VAL_CANCELLED_IN_QUEUE;
  static const std::string PROFILE_INFO_VAL_ADMIT_QUEUED;
  static const std::string PROFILE_INFO_VAL_REJECTED;
  static const std::string PROFILE_INFO_VAL_TIME_OUT;
  static const std::string PROFILE_INFO_KEY_INITIAL_QUEUE_REASON;
  static const std::string PROFILE_INFO_VAL_INITIAL_QUEUE_REASON;
  static const std::string PROFILE_INFO_KEY_LAST_QUEUED_REASON;
  static const std::string PROFILE_INFO_KEY_ADMITTED_MEM;
  static const std::string PROFILE_INFO_KEY_EXECUTOR_GROUP;
  static const std::string PROFILE_INFO_KEY_STALENESS_WARNING;
  static const std::string PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME;

  AdmissionController(ClusterMembershipMgr* cluster_membership_mgr,
      StatestoreSubscriber* subscriber, RequestPoolService* request_pool_service,
      MetricGroup* metrics, const TNetworkAddress& host_addr);
  ~AdmissionController();

  /// This struct contains all information needed to create a QuerySchedule and try to
  /// admit it. None of the members are owned by the instances of this class (usually they
  /// are owned by the ClientRequestState).
  struct AdmissionRequest {
    const TUniqueId& query_id;
    const TQueryExecRequest& request;
    const TQueryOptions& query_options;
    RuntimeProfile* summary_profile;
    RuntimeProfile::EventSequence* query_events;
  };

  /// Submits the request for admission. May returns immediately if rejected, but
  /// otherwise blocks until the request is either admitted, times out, gets rejected
  /// later, or cancelled by the client (by setting 'admit_outcome' to CANCELLED). When
  /// this method returns, the following <admit_outcome, Return Status> pairs are
  /// possible:
  /// - Admitted: <ADMITTED, Status::OK>
  /// - Rejected or timed out: <REJECTED or TIMED_OUT, Status(msg: reason for the same)>
  /// - Cancelled: <CANCELLED, Status::CANCELLED>
  /// If admitted, ReleaseQuery() should also be called after the query completes or gets
  /// cancelled to ensure that the pool statistics are updated.
  Status SubmitForAdmission(const AdmissionRequest& request,
      Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admit_outcome,
      std::unique_ptr<QuerySchedule>* schedule_result);

  /// Updates the pool statistics when a query completes (either successfully,
  /// is cancelled or failed). This should be called for all requests that have
  /// been submitted via AdmitQuery(). 'schedule' is the QuerySchedule of the completed
  /// query and 'peak_mem_consumption' is the peak memory consumption of the query.
  /// This does not block.
  void ReleaseQuery(const QuerySchedule& schedule, int64_t peak_mem_consumption);

  /// Updates the pool statistics when a Backend running a query completes (either
  /// successfully, is cancelled or failed). This should be called for all Backends part
  /// of a query for all queries that have been submitted via AdmitQuery().
  /// 'schedule' is the QuerySchedule of the associated query and the vector of
  /// TNetworkAddresses identify the completed Backends.
  /// This does not block.
  void ReleaseQueryBackends(
      const QuerySchedule& schedule, const vector<TNetworkAddress>& host_addr);

  /// Registers the request queue topic with the statestore, starts up the dequeue thread
  /// and registers a callback with the cluster membership manager to receive updates for
  /// membership changes.
  Status Init();

  /// Serializes relevant stats, configurations and information associated with queued
  /// queries for the resource pool identified by 'pool_name' to JSON by adding members to
  /// 'resource_pools'. Is a no-op if a pool with name 'pool_name' does not exist or no
  /// queries have been submitted to that pool yet.
  void PoolToJson(const std::string& pool_name, rapidjson::Value* resource_pools,
      rapidjson::Document* document);

  /// Serializes relevant stats, configurations and information associated with queued
  /// queries for every resource pool (to which queries have been submitted at least once)
  /// to JSON by adding members to 'resource_pools'.
  void AllPoolsToJson(rapidjson::Value* resource_pools, rapidjson::Document* document);

  /// Calls ResetInformationalStats on the pool identified by 'pool_name'.
  void ResetPoolInformationalStats(const std::string& pool_name);

  /// Calls ResetInformationalStats on all pools.
  void ResetAllPoolInformationalStats();

  // This struct stores per-host statistics which are used during admission and by HTTP
  // handlers to query admission control statistics for currently registered backends.
  struct HostStats {
    /// The mem reserved for a query that is currently executing is its memory limit, if
    /// set (which should be the common case with admission control). Otherwise, if the
    /// query has no limit or the query is finished executing, the current consumption
    /// (tracked by its query mem tracker) is used.
    int64_t mem_reserved = 0;
    /// The per host mem admitted only for the queries admitted locally.
    int64_t mem_admitted = 0;
    /// The per host number of queries admitted only for the queries admitted locally.
    int64_t num_admitted = 0;
  };

  typedef std::unordered_map<std::string, HostStats> PerHostStats;

  // Populates the input map with the per host memory reserved and admitted in the
  // following format: <host_address_str, pair<mem_reserved, mem_admitted>>.
  // Only used for populating the 'backends' debug page.
  void PopulatePerHostMemReservedAndAdmitted(PerHostStats* host_stats);

  /// Returns a non-empty string with a warning if the admission control data is stale.
  /// 'prefix' is added to the start of the string. Returns an empty string if not stale.
  /// If 'ms_since_last_update' is non-null, set it to the time in ms since last update.
  /// Caller must not hold 'admission_ctrl_lock_'.
  std::string GetStalenessDetail(const std::string& prefix,
      int64_t* ms_since_last_update = nullptr);

 private:
  class PoolStats;
  friend class PoolStats;

  /// Pointer to the cluster membership manager. Not owned by the AdmissionController.
  ClusterMembershipMgr* cluster_membership_mgr_;

  /// Subscription manager used to handle admission control updates. This is not
  /// owned by this class.
  StatestoreSubscriber* subscriber_;

  /// Used for user-to-pool resolution and looking up pool configurations. Not owned by
  /// the AdmissionController.
  RequestPoolService* request_pool_service_;

  /// Metrics subsystem access
  MetricGroup* metrics_group_;

  /// Maps names of executor groups to their respective query load metric.
  std::unordered_map<std::string, IntGauge*> exec_group_query_load_map_;

  /// Thread dequeuing and admitting queries.
  std::unique_ptr<Thread> dequeue_thread_;

  // The local impalad's host/port id, used to construct topic keys.
  const std::string host_id_;

  /// Serializes/deserializes TPoolStats when sending and receiving topic updates.
  ThriftSerializer thrift_serializer_;

  /// Protects all access to all variables below.
  boost::mutex admission_ctrl_lock_;

  /// The last time a topic update was processed. Time is obtained from
  /// MonotonicMillis(), or is 0 if an update was never received.
  int64_t last_topic_update_time_ms_ = 0;

  PerHostStats host_stats_;

  /// Contains all per-pool statistics and metrics. Accessed via GetPoolStats().
  class PoolStats {
   public:
    struct PoolMetrics {
      /// Monotonically increasing counters (since process start) referring to this
      /// host's admission controller.
      IntCounter* total_admitted;
      IntCounter* total_rejected;
      IntCounter* total_queued;
      IntCounter* total_dequeued; // Does not include those in total_timed_out
      IntCounter* total_timed_out;
      IntCounter* total_released;
      IntCounter* time_in_queue_ms;

      /// The following mirror the current values in PoolStats.
      /// TODO: Avoid duplication: replace the int64_t fields on PoolStats with these.
      IntGauge* agg_num_running;
      IntGauge* agg_num_queued;
      IntGauge* agg_mem_reserved;
      IntGauge* local_mem_admitted;

      /// The following mirror the current values of local_stats_.
      /// TODO: As above, consolidate the metrics and local_stats_.
      IntGauge* local_num_admitted_running;
      IntGauge* local_num_queued;
      IntGauge* local_backend_mem_reserved;
      IntGauge* local_backend_mem_usage;

      /// Metrics exposing the pool settings.
      IntGauge* pool_max_mem_resources;
      IntGauge* pool_max_requests;
      IntGauge* pool_max_queued;
      IntGauge* pool_queue_timeout;
      IntGauge* max_query_mem_limit;
      IntGauge* min_query_mem_limit;
      BooleanProperty* clamp_mem_limit_query_option;
      DoubleGauge* max_running_queries_multiple;
      DoubleGauge* max_queued_queries_multiple;
      IntGauge* max_memory_multiple;
      /// Metrics exposing the pool's derived runtime configuration.
      IntGauge* max_running_queries_derived;
      IntGauge* max_queued_queries_derived;
      IntGauge* max_memory_derived;
    };

    PoolStats(AdmissionController* parent, const std::string& name)
      : name_(name), parent_(parent), agg_num_running_(0), agg_num_queued_(0),
        agg_mem_reserved_(0), local_mem_admitted_(0), wait_time_ms_ema_(0.0) {
      peak_mem_histogram_.resize(HISTOGRAM_NUM_OF_BINS, 0);
      InitMetrics();
    }

    int64_t agg_num_running() const { return agg_num_running_; }
    int64_t agg_num_queued() const { return agg_num_queued_; }
    int64_t EffectiveMemReserved() const {
      return std::max(agg_mem_reserved_, local_mem_admitted_);
    }

    // ADMISSION LIFECYCLE METHODS
    /// Updates the pool stats when the request represented by 'schedule' is admitted.
    void AdmitQueryAndMemory(const QuerySchedule& schedule);
    /// Updates the pool stats except the memory admitted stat.
    void ReleaseQuery(int64_t peak_mem_consumption);
    /// Releases the specified memory from the pool stats.
    void ReleaseMem(int64_t mem_to_release);
    /// Updates the pool stats when the request represented by 'schedule' is queued.
    void Queue();
    /// Updates the pool stats when the request represented by 'schedule' is dequeued.
    void Dequeue(bool timed_out);

    // STATESTORE CALLBACK METHODS
    /// Updates the local_stats_.backend_mem_reserved with the pool mem tracker. Called
    /// before sending local_stats().
    void UpdateMemTrackerStats();

    /// Called on a full topic update to clear all stats before processing the update.
    void ClearRemoteStats() { remote_stats_.clear(); }

    /// Called to update remote host TPoolStats with the new host_stats for the
    /// specified host. If host_stats is NULL the stats for the specified remote host
    /// are removed (i.e. topic deletion).
    void UpdateRemoteStats(const std::string& backend_id, TPoolStats* host_stats);

    /// Maps from host id to memory reserved and memory admitted, both aggregates over all
    /// pools. See the class doc for a detailed definition of reserved and admitted.
    /// Protected by admission_ctrl_lock_.
    typedef boost::unordered_map<std::string, int64_t> HostMemMap;

    /// Called after updating local_stats_ and remote_stats_ to update the aggregate
    /// values of agg_num_running_, agg_num_queued_, and agg_mem_reserved_. The in/out
    /// parameter host_mem_reserved is a map from host id to memory reserved used to
    /// aggregate the mem reserved values across all pools for each host. Used by
    /// UpdateClusterAggregates() to update host_mem_reserved_; it provides the host
    /// aggregates when called over all pools.
    void UpdateAggregates(HostMemMap* host_mem_reserved);

    const TPoolStats& local_stats() { return local_stats_; }

    /// Updates the metrics exposing the pool configuration to those in pool_cfg.
    void UpdateConfigMetrics(const TPoolConfig& pool_cfg, int64_t cluster_size);

    /// Updates the metrics exposing the scalable pool configuration values.
    void UpdateDerivedMetrics(const TPoolConfig& pool_cfg, int64_t cluster_size);

    PoolMetrics* metrics() { return &metrics_; }
    std::string DebugString() const;

    /// Updates the metric keeping track of total time in queue and the exponential
    /// moving average of query wait time for all queries submitted to this pool.
    void UpdateWaitTime(int64_t wait_time_ms);

    /// Serializes relevant stats and configurations to JSON by adding members to 'pool'.
    void ToJson(rapidjson::Value* pool, rapidjson::Document* document) const;

    /// Resets the informational stats like those keeping track of absolute
    /// values(totals), the peak query memory histogram, and the exponential moving
    /// average of wait time.
    void ResetInformationalStats();

    const std::string& name() const { return name_; }

   private:
    const std::string name_;
    AdmissionController* parent_;

    /// Aggregate (across all hosts) number of running queries in this pool. Updated
    /// by Admit(), Release(), and after processing statestore updates by
    /// UpdateAggregates().
    int64_t agg_num_running_;

    /// Aggregate (across all hosts) number of queued requests. Updated by Queue(),
    /// Dequeue(), and after processing statestore updates by UpdateAggregates().
    int64_t agg_num_queued_;

    /// Aggregate memory reported as reserved for fragments executing in this pool by
    /// every host, i.e. the sum of all local_stats_.mem_reserved from all
    /// other hosts. Updated only by UpdateAggregates().
    int64_t agg_mem_reserved_;

    /// Memory in this pool (across all nodes) that is needed for requests that have been
    /// admitted by this local coordinator. Updated only on Admit() and Release(). Stored
    /// separately from the other 'local' stats in local_stats_ because it is not sent
    /// to the statestore (no 'aggregated' value is needed).
    int64_t local_mem_admitted_;

    /// This pool's TPoolStats for this host. Sent to the statestore (and thus not stored
    /// in remote_stats_ with the remote hosts). Most fields are updated eagerly and used
    /// for local admission decisions. local_stats_.backend_mem_reserved is the
    /// exception: it is not used in local admission decisions so it can be updated
    /// lazily before sending a statestore update.
    TPoolStats local_stats_;

    /// Map of host_ids to the latest TPoolStats. Entirely generated by incoming
    /// statestore updates; updated by UpdateRemoteStats() and used by UpdateAggregates().
    typedef boost::unordered_map<std::string, TPoolStats> RemoteStatsMap;
    RemoteStatsMap remote_stats_;

    /// Per-pool metrics, created by InitMetrics().
    PoolMetrics metrics_;

    /// A histogram of the peak memory used by a query among all hosts. Its a vector of
    /// size 'HISTOGRAM_NUM_OF_BINS' and every i-th element represents the number of
    /// queries that had recorded a peak memory between (i, i+1] * HISTOGRAM_BIN_SIZE
    /// Bytes, except for the last one that represents a memory range of
    /// (HISTOGRAM_NUM_OF_BINS - 1, infinity) * HISTOGRAM_BIN_SIZE Bytes.
    std::vector<int64_t> peak_mem_histogram_;
    static const int64_t HISTOGRAM_NUM_OF_BINS;
    static const int64_t HISTOGRAM_BIN_SIZE;

    /// Keeps track of exponential moving average of all queries submitted to this pool
    /// that were not rejected. A weighting multiplier of value 'EMA_MULTIPLIER' is used.
    double wait_time_ms_ema_;
    static const double EMA_MULTIPLIER;

    void InitMetrics();

    FRIEND_TEST(AdmissionControllerTest, Simple);
    FRIEND_TEST(AdmissionControllerTest, PoolStats);
    FRIEND_TEST(AdmissionControllerTest, CanAdmitRequestMemory);
    FRIEND_TEST(AdmissionControllerTest, CanAdmitRequestCount);
    FRIEND_TEST(AdmissionControllerTest, GetMaxToDequeue);
    FRIEND_TEST(AdmissionControllerTest, QueryRejection);
    friend class AdmissionControllerTest;
  };

  /// Map of pool names to pool stats. Accessed via GetPoolStats().
  /// Protected by admission_ctrl_lock_.
  typedef boost::unordered_map<std::string, PoolStats> PoolStatsMap;
  PoolStatsMap pool_stats_;

  /// This struct groups together a schedule and the executor group that it was scheduled
  /// on. It is used to attempt admission without rescheduling the query in case the
  /// cluster membership has not changed. Users of the struct must make sure that
  /// executor_group stays valid.
  struct GroupSchedule {
    GroupSchedule(
        std::unique_ptr<QuerySchedule> schedule, const ExecutorGroup& executor_group)
      : schedule(std::move(schedule)), executor_group(executor_group) {}
    std::unique_ptr<QuerySchedule> schedule;
    const ExecutorGroup& executor_group;
  };

  /// The set of pools that have changed between topic updates that need stats to be sent
  /// to the statestore. The key is the pool name.
  typedef boost::unordered_set<std::string> PoolSet;
  PoolSet pools_for_updates_;

  /// Structure stored in the RequestQueue representing an admission request. This struct
  /// lives only during the call to AdmitQuery() but its members live past that and are
  /// owned by the ClientRequestState object associated with them.
  ///
  /// Objects of this class progress linearly through the following states.
  /// - Initialized: The request has been created
  /// - Admitting: The request has been attempted to be admitted at least once and
  ///   additional intermediate state has been stored in some members
  /// - Admitted: The request was admitted, cancelled, or rejected and 'admit_outcome' is
  ///   set. If it was admitted, 'admitted_schedule' is also not nullptr.
  struct QueueNode : public InternalQueue<QueueNode>::Node {
    QueueNode(AdmissionRequest request,
        Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admission_outcome,
        RuntimeProfile* profile)
      : admission_request(std::move(request)),
        profile(profile),
        admit_outcome(admission_outcome) {}

    /////////////////////////////////////////
    /// BEGIN: Members that are valid for new objects after initialization

    /// The admission request contains everything required to build schedules.
    const AdmissionRequest admission_request;

    /// Profile to be updated with information about admission.
    RuntimeProfile* profile;

    /// END: Members that are valid for new objects after initialization
    /////////////////////////////////////////

    /////////////////////////////////////////
    /// BEGIN: Members that are only valid while queued, but invalid once dequeued.

    /// The membership snapshot used during the last admission attempt. It can be nullptr
    /// before the first admission attempt and if any schedules have been created,
    /// 'group_schedule' will contain the corresponding schedules and executor groups.
    ClusterMembershipMgr::SnapshotPtr membership_snapshot;

    /// List of schedules and executor groups that can be attempted to be admitted for
    /// this queue node.
    std::vector<GroupSchedule> group_schedules;

    /// END: Members that are only valid while queued, but invalid once dequeued.
    /////////////////////////////////////////

    /////////////////////////////////////////
    /// BEGIN: Members that are valid after admission / cancellation / rejection

    /// The last reason why this request could not be admitted.
    std::string not_admitted_reason;

    /// The Admission outcome of the queued request.
    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* const admit_outcome;

    /// The schedule of the query if it was admitted successfully. Nullptr if it has not
    /// been admitted or was cancelled or rejected.
    std::unique_ptr<QuerySchedule> admitted_schedule = nullptr;

    /// END: Members that are valid after admission / cancellation / rejection
    /////////////////////////////////////////
  };

  /// Queue for the queries waiting to be admitted for execution. Once the
  /// maximum number of concurrently executing queries has been reached,
  /// incoming queries are queued and admitted first come, first served.
  typedef InternalQueue<QueueNode> RequestQueue;

  /// Map of pool names to request queues.
  typedef boost::unordered_map<std::string, RequestQueue> RequestQueueMap;
  RequestQueueMap request_queue_map_;

  /// Map of pool names to the pool configs returned by request_pool_service_. Stored so
  /// that the dequeue thread does not need to access the configs via the request pool
  /// service again (which involves a JNI call and error checking).
  typedef boost::unordered_map<std::string, TPoolConfig> PoolConfigMap;
  PoolConfigMap pool_config_map_;

  /// Notifies the dequeuing thread that pool stats have changed and it may be
  /// possible to dequeue and admit queries.
  ConditionVariable dequeue_cv_;

  /// If true, tear down the dequeuing thread. This only happens in unit tests.
  bool done_;

  /// Tracks the number of released Backends for each active query. Used purely for
  /// internal state validation. Used to ensure that all Backends are released before
  /// the query is released.
  typedef boost::unordered_map<TUniqueId, int> NumReleasedBackends;
  NumReleasedBackends num_released_backends_;

  /// Resolves the resource pool name in 'query_ctx.request_pool' and stores the resulting
  /// name in 'pool_name' and the resulting config in 'pool_config'.
  Status ResolvePoolAndGetConfig(const TQueryCtx& query_ctx, std::string* pool_name,
      TPoolConfig* pool_config);

  /// Statestore subscriber callback that sends outgoing topic deltas (see
  /// AddPoolUpdates()) and processes incoming topic deltas, updating the PoolStats
  /// state.
  void UpdatePoolStats(
      const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Adds outgoing topic updates to subscriber_topic_updates for pools that have changed
  /// since the last call to AddPoolUpdates(). Called by UpdatePoolStats() before
  /// UpdateClusterAggregates(). Must hold admission_ctrl_lock_.
  void AddPoolUpdates(std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Updates the remote stats with per-host topic_updates coming from the statestore.
  /// Removes remote stats identified by topic deletions coming from the
  /// statestore. Called by UpdatePoolStats(). Must hold admission_ctrl_lock_.
  void HandleTopicUpdates(const std::vector<TTopicItem>& topic_updates);

  /// Re-computes the per-pool aggregate stats and the per-host aggregates in host_stats_
  /// using each pool's remote_stats_ and local_stats_.
  /// Called by UpdatePoolStats() after handling updates and deletions.
  /// Must hold admission_ctrl_lock_.
  void UpdateClusterAggregates();

  /// Computes schedules for all executor groups that can run the query in 'queue_node'.
  /// For subsequent calls schedules are only re-computed if the membership version inside
  /// 'membership_snapshot' has changed. Will return any errors that occur during
  /// scheduling, e.g. if the scan range generation fails. Note that this will not return
  /// an error if no executor groups are available for scheduling, but will set
  /// 'queue_node->not_admitted_reason' and leave 'queue_node->group_schedules' empty in
  /// that case.
  Status ComputeGroupSchedules(
      ClusterMembershipMgr::SnapshotPtr membership_snapshot, QueueNode* queue_node);

  /// Reschedules the query if necessary using 'membership_snapshot' and tries to find an
  /// executor group that the query can be admitted to. If the query is unable to run on
  /// any of the groups irrespective of their current workload, it is rejected. Returns
  /// true and sets queue_node->admitted_schedule if the query can be admitted. Returns
  /// true and keeps queue_node->admitted_schedule unset if the query cannot be admitted
  /// now, but also does not need to be rejected. If the query must be rejected, this
  /// method returns false and sets queue_node->not_admitted_reason.
  bool FindGroupToAdmitOrReject(
      int64_t cluster_size, ClusterMembershipMgr::SnapshotPtr membership_snapshot,
      const TPoolConfig& pool_config, bool admit_from_queue, PoolStats* pool_stats,
      QueueNode* queue_node);

  /// Dequeues the queued queries when notified by dequeue_cv_ and admits them if they
  /// have not been cancelled yet.
  void DequeueLoop();

  /// Returns true if schedule can be admitted to the pool with pool_cfg.
  /// admit_from_queue is true if attempting to admit from the queue. Otherwise, returns
  /// false and not_admitted_reason specifies why the request can not be admitted
  /// immediately. Caller owns not_admitted_reason. Must hold admission_ctrl_lock_.
  bool CanAdmitRequest(const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
      int64_t cluster_size, bool admit_from_queue, std::string* not_admitted_reason);

  /// Returns true if all executors can accommodate the largest initial reservation of
  /// any executor and the backend running the coordinator fragment can accommodate its
  /// own initial reservation. Otherwise, returns false with the details about the memory
  /// shortage in 'mem_unavailable_reason'. Possible cases where it can return false are:
  /// 1. The pool.max_query_mem_limit is set too low
  /// 2. mem_limit in query options is set low and no max/min_query_mem_limit is set in
  ///    the pool configuration.
  /// 3. mem_limit in query options is set low and min_query_mem_limit is also set low.
  /// 4. mem_limit in query options is set low and the pool.min_query_mem_limit is set
  ///    to a higher value but pool.clamp_mem_limit_query_option is false.
  /// 5. If a dedicated coordinator is used and the mem_limit in query options is set
  ///    lower than what is required to support the sum of initial memory reservations of
  ///    the fragments scheduled on the coordinator.
  static bool CanAccommodateMaxInitialReservation(const QuerySchedule& schedule,
      const TPoolConfig& pool_cfg, std::string* mem_unavailable_reason);

  /// Returns true if there is enough memory available to admit the query based on the
  /// schedule, the aggregate pool memory, and the per-host memory. If not, this returns
  /// false and returns the reason in 'mem_unavailable_reason'. Caller owns
  /// 'mem_unavailable_reason'.
  /// Must hold admission_ctrl_lock_.
  bool HasAvailableMemResources(const QuerySchedule& schedule,
      const TPoolConfig& pool_cfg, int64_t cluster_size,
      std::string* mem_unavailable_reason);

  /// Returns true if there is an available slot on all executors in the schedule. The
  /// number of slots per executors does not change with the group or cluster size and
  /// instead always uses pool_cfg.max_requests. If a host does not have a free slot, this
  /// returns false and sets 'unavailable_reason'.
  /// Must hold admission_ctrl_lock_.
  bool HasAvailableSlot(const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
      string* unavailable_reason);

  /// Updates the memory admitted and the num of queries running for each backend in
  /// 'schedule'. Also updates the stats of its associated resource pool. Used only when
  /// the 'schedule' is admitted.
  void UpdateStatsOnAdmission(const QuerySchedule& schedule);

  /// Updates the memory admitted and the num of queries running for each backend in
  /// 'schedule' which have been release/completed. The list of completed backends is
  /// specified in 'host_addrs'. Also updates the stats related to the admitted memory of
  /// its associated resource pool.
  void UpdateStatsOnReleaseForBackends(
      const QuerySchedule& schedule, const std::vector<TNetworkAddress>& host_addrs);

  /// Updates the memory admitted and the num of queries running on the specified host by
  /// adding the specified mem and num_queries to the host stats.
  void UpdateHostStats(
      const TNetworkAddress& host_addr, int64_t mem_to_admit, int num_queries_to_admit);

  /// Rejection happens in several stages
  /// 1) Based on static pool configuration
  ///     - Check if the pool is disabled (max_requests = 0, max_mem = 0)
  ///     - min_query_mem_limit > max_query_mem_limit (From IsPoolConfigValidForCluster)
  ///
  /// 2) Based on the entire cluster size
  ///     - Check for maximum queue size (queue full)
  ///
  /// 3) Based on the executor group size
  ///     - pool.min_query_mem_limit > max_mem (From IsPoolConfigValidForCluster)
  ///       - max_mem may depend on group size
  ///
  /// 4) Based on a schedule
  ///     - largest_min_mem_reservation > buffer_pool_limit
  ///     - CanAccommodateMaxInitialReservation
  ///     - Thread reservation limit (thread_reservation_limit,
  ///       thread_reservation_aggregate_limit)
  ///     - cluster_min_mem_reservation_bytes > max_mem
  ///     - cluster_mem_to_admit > max_mem
  ///     - per_backend_mem_to_admit > min_admit_mem_limit
  ///
  /// We lump together 1 & 2 and 3 & 4. The first two depend on the total cluster size.
  /// The latter 2 depend on the executor group size and therefore on the schedule. If no
  /// executor group is available, the query will be queued.

  /// Returns true if a request must be rejected immediately based on the pool
  /// configuration and cluster size, e.g. if the pool config is invalid, the pool is
  /// disabled, or the queue is already full.
  /// Must hold admission_ctrl_lock_.
  bool RejectForCluster(const std::string& pool_name, const TPoolConfig& pool_cfg,
      bool admit_from_queue, int64_t cluster_size, std::string* rejection_reason);

  /// Returns true if a request must be rejected immediately based on the pool
  /// configuration and a particular schedule, e.g. because the memory requirements of the
  /// query exceed the maximum of the group. This assumes that all executor groups for a
  /// pool are uniform and that a query rejected for one group will not be able to run on
  /// other groups, either.
  /// Must hold admission_ctrl_lock_.
  bool RejectForSchedule(const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
      int64_t cluster_size, int64_t group_size, std::string* rejection_reason);

  /// Gets or creates the PoolStats for pool_name. Must hold admission_ctrl_lock_.
  PoolStats* GetPoolStats(const std::string& pool_name, bool dcheck_exists = false);

  /// Gets or creates the PoolStats for query schedule 'schedule'. Scheduling must be done
  /// already and the schedule must have an associated executor_group.
  PoolStats* GetPoolStats(const QuerySchedule& schedule);

  /// Log the reason for dequeuing of 'node' failing and add the reason to the query's
  /// profile. Must hold admission_ctrl_lock_.
  static void LogDequeueFailed(QueueNode* node, const std::string& not_admitted_reason);

  /// Sets the per host mem limit and mem admitted in the schedule and does the necessary
  /// accounting and logging on successful submission.
  /// Caller must hold 'admission_ctrl_lock_'.
  void AdmitQuery(QuerySchedule* schedule, bool was_queued);

  /// Same as PoolToJson() but requires 'admission_ctrl_lock_' to be held by the caller.
  /// Is a helper method used by both PoolToJson() and AllPoolsToJson()
  void PoolToJsonLocked(const std::string& pool_name, rapidjson::Value* resource_pools,
      rapidjson::Document* document);

  /// Same as GetStalenessDetail() except caller must hold 'admission_ctrl_lock_'.
  std::string GetStalenessDetailLocked(const std::string& prefix,
      int64_t* ms_since_last_update = nullptr);

  /// Returns the topic key for the pool at this backend, i.e. a string of the
  /// form: "<pool_name><delimiter><backend_id>".
  static std::string MakePoolTopicKey(
      const std::string& pool_name, const std::string& backend_id);

  /// Returns the maximum memory for the pool.
  static int64_t GetMaxMemForPool(const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns a description of how the maximum memory for the pool is configured.
  static std::string GetMaxMemForPoolDescription(
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns the maximum number of requests that can run in the pool.
  static int64_t GetMaxRequestsForPool(
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns a description of how the maximum number of requests that can run in the pool
  /// is configured.
  static std::string GetMaxRequestsForPoolDescription(
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns the effective queue timeout for the pool in milliseconds.
  static int64_t GetQueueTimeoutForPoolMs(const TPoolConfig& pool_config);

  /// Returns a maximum number of queries that should be dequeued locally from 'queue'
  /// before DequeueLoop waits on dequeue_cv_ at the top of its loop.
  /// If it can be determined that no queries can currently be run, then zero
  /// is returned.
  /// Uses a heuristic to limit the number of requests we dequeue locally to avoid all
  /// impalads dequeuing too many requests at the same time.
  int64_t GetMaxToDequeue(RequestQueue& queue, PoolStats* stats,
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns true if the pool has been disabled through configuration.
  static bool PoolDisabled(const TPoolConfig& pool_config);

  /// Returns true if the pool is configured to limit the number of running queries.
  static bool PoolLimitsRunningQueriesCount(const TPoolConfig& pool_config);

  /// Returns true if the pool has a fixed (i.e. not scalable) maximum memory limit.
  static bool PoolHasFixedMemoryLimit(const TPoolConfig& pool_config);

  /// Returns the maximum number of requests that can be queued in the pool.
  static int64_t GetMaxQueuedForPool(
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Returns a description of how the maximum number of requests that can run be queued
  /// in the pool is configured.
  static std::string GetMaxQueuedForPoolDescription(
      const TPoolConfig& pool_config, int64_t cluster_size);

  /// Return all executor groups from 'all_groups' that can be used to run queries in
  /// 'pool_name'.
  void GetExecutorGroupsForPool(const ClusterMembershipMgr::ExecutorGroups& all_groups,
      const std::string& pool_name, std::vector<const ExecutorGroup*>* matching_groups);

  /// Returns the current size of the cluster.
  int64_t GetClusterSize(const ClusterMembershipMgr::Snapshot& membership_snapshot);

  /// Returns the size of executor group 'group_name' in 'membership_snapshot'.
  int64_t GetExecutorGroupSize(const ClusterMembershipMgr::Snapshot& membership_snapshot,
      const std::string& group_name);

  /// Get the amount of memory to admit for the Backend with the given BackendExecParams.
  /// This method may return different values depending on if the Backend is an Executor
  /// or a Coordinator.
  static int64_t GetMemToAdmit(
      const QuerySchedule& schedule, const BackendExecParams& backend_exec_params);

  /// Updates the list of executor groups for which we maintain the query load metrics.
  /// Removes the metrics of the groups that no longer exist from the metric group and
  /// adds new ones for the newly added groups.
  void UpdateExecGroupMetricMap(ClusterMembershipMgr::SnapshotPtr snapshot);

  /// Updates the num queries executing metric of the 'grp_name' executor group by
  /// 'delta'. Only updates it if the metric exists ('grp_name' has non-zero executors).
  /// Caller must hold 'admission_ctrl_lock_'. Must be called whenever a query is
  /// admitted or released.
  void UpdateExecGroupMetric(const string& grp_name, int64_t delta);

  FRIEND_TEST(AdmissionControllerTest, Simple);
  FRIEND_TEST(AdmissionControllerTest, PoolStats);
  FRIEND_TEST(AdmissionControllerTest, CanAdmitRequestMemory);
  FRIEND_TEST(AdmissionControllerTest, CanAdmitRequestCount);
  FRIEND_TEST(AdmissionControllerTest, GetMaxToDequeue);
  FRIEND_TEST(AdmissionControllerTest, QueryRejection);
  FRIEND_TEST(AdmissionControllerTest, DedicatedCoordQuerySchedule);
  FRIEND_TEST(AdmissionControllerTest, DedicatedCoordAdmissionChecks);
  friend class AdmissionControllerTest;
};

} // namespace impala

#endif // SCHEDULING_ADMISSION_CONTROLLER_H
