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

#include <vector>
#include <string>
#include <list>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/query-schedule.h"
#include "statestore/statestore-subscriber.h"
#include "util/condition-variable.h"
#include "util/internal-queue.h"
#include "util/thread.h"

namespace impala {

class QuerySchedule;
class ExecEnv;

/// The AdmissionController is used to throttle requests (e.g. queries, DML) based
/// on available cluster resources, which are configured in one or more resource pools. A
/// request will either be admitted for immediate execution, queued for later execution,
/// or rejected.  Resource pools can be configured to have maximum number of concurrent
/// queries, maximum memory, and a maximum queue size. Queries will be queued if there
/// are already too many queries executing or there isn't enough available memory. Once
/// the queue reaches the maximum queue size, incoming queries will be rejected. Requests
/// in the queue will time out after a configurable timeout.
///
/// Any impalad can act as a coordinator and thus also an admission controller, so some
/// cluster state must be shared between impalads in order to make admission decisions on
/// any node. Every impalad maintains some per-pool and per-host statistics related to
/// the requests it itself is servicing as the admission controller. Some of these
/// local admission statistics in addition to some backend-specific statistics (i.e.
/// the backend executor associated with the same impalad process) are disseminated
/// across the cluster via the statestore using the IMPALA_REQUEST_QUEUE_TOPIC topic.
/// For example, coordinators will end up sending statestore updates where the admission
/// statistics reflect the load and all participating backends will have statestore
/// updates reflecting load they're executing.
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
/// MEM_LIMIT (either explicitly or via a default value). This is a per-node value. If
/// there is no memory limit, the per-node estimate from planning is used instead. The
/// following two conditions must hold in order for the request to be admitted:
///  1) There must be enough memory resources available in this resource pool for the
///     request. The max memory resources configured for the resource pool specifies the
///     aggregate, cluster-wide memory that may be reserved by all executing queries in
///     this pool. Thus the aggregate memory to be reserved across all participating
///     backends for this request, *plus* that of already admitted requests must be less
///     than or equal to the max resources specified.
///  2) All participating backends must have enough memory available. Each impalad has a
///     per-process mem limit, and that is the max memory that can be reserved on that
///     backend.
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
///     when requests are admitted and released (and note: not via the statestore, so
///     there is no latency, but this does not account for memory from requests admitted
///     by other impalads).
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
/// Example:
/// Consider a 10-node cluster with 100gb/node and a resource pool 'q1' configured with
/// 500gb of memory. An incoming request with a 40gb MEM_LIMIT and schedule to execute on
/// all backends is received by AdmitQuery() on an otherwise quiet cluster.
/// CanAdmitRequest() checks the number of running queries and then calls
/// HasAvailableMemResources() to check for memory resources. It first checks whether
/// there is enough memory for the request using PoolStats::EffectiveMemReserved() (which
/// is the max of the pool's agg_mem_reserved_ and local_mem_admitted_, see #1 above),
/// and then checks for enough memory on each individual host via the max of the values
/// in the host_mem_reserved_ and host_mem_admitted_ maps (see #2 above). In this case,
/// ample resources are available so CanAdmitRequest() returns true.  PoolStats::Admit()
/// is called to update q1's PoolStats: it first updates agg_num_running_ and
/// local_mem_admitted_ which are able to be used immediately for incoming admission
/// requests, then it updates num_admitted_running in the struct sent to the statestore
/// (local_stats_). UpdateHostMemAdmitted() is called to update the per-host admitted mem
/// (stored in the map host_mem_admitted_) for all participating hosts. Then AdmitQuery()
/// returns to the Scheduler. If another identical admission request is received by the
/// same coordinator immediately, it will be rejected because q1's local_mem_admitted_ is
/// already 400gb. If that request were sent to another impalad at the same time, it
/// would have been admitted because not all updates have been disseminated yet. The next
/// statestore update will contain the updated value of num_admitted_running for q1 on
/// this backend. As remote fragments begin execution on remote impalads, their pool mem
/// trackers will reflect the updated amount of memory reserved (set in
/// local_stats_.backend_mem_reserved by UpdateMemTrackerStats()) and the next statestore
/// updates coming from those impalads will send the updated value.  As the statestore
/// updates are received (in the subscriber callback fn UpdatePoolStats()), the incoming
/// per-backend, per-pool mem_reserved values are aggregated to
/// PoolStats::agg_mem_reserved_ (pool aggregate over all hosts) and
/// backend_mem_reserved_ (per-host aggregates over all pools). Once this has happened,
/// any incoming admission request now has the updated state required to make correct
/// admission decisions.
///
/// Queuing Behavior:
/// Once the resources in a pool are consumed each coordinator receiving requests will
/// begin queuing. While each individual queue is FIFO, there is no total ordering on the
/// queued requests between admission controllers and no FIFO behavior is guaranteed for
/// requests submitted to different coordinators. When resources become available, there
/// is no synchronous coordination between nodes used to determine which get to dequeue and
/// admit requests. Instead, we use a simple heuristic to try to dequeue a number of
/// requests proportional to the number of requests that are waiting in each individual
/// admission controller to the total number of requests queued across all admission
/// controllers (i.e. impalads). This limits the amount of overadmission that may result
/// from a large amount of resources becoming available at the same time.
/// When there are requests queued in multiple pools on the same host, the admission
/// controller simply iterates over the pools in pool_stats_ and attempts to dequeue from
/// each. This is fine for the max_requests limit, but is unfair for memory-based
/// admission because the iteration order of pools effectively gives priority to the
/// queues at the beginning. Requests across queues may be competing for the same
/// resources on particular hosts, i.e. #2 in the description of memory-based admission
/// above. Note the pool's max_mem_resources (#1) is not contented.
/// TODO: Improve the dequeuing policy. IMPALA-2968.
///
/// TODO: Assumes all impalads have the same proc mem limit. Should send proc mem limit
///       via statestore (e.g. ideally in TBackendDescriptor) and check per-node
///       reservations against this value.
/// TODO: Remove less important debug logging after more cluster testing. Should have a
///       better idea of what is perhaps unnecessary.
class AdmissionController {
 public:
  AdmissionController(StatestoreSubscriber* subscriber,
      RequestPoolService* request_pool_service, MetricGroup* metrics,
      const TNetworkAddress& host_addr);
  ~AdmissionController();

  /// Submits the request for admission. Returns immediately if rejected, but
  /// otherwise blocks until the request is admitted. When this method returns,
  /// schedule->is_admitted() is true if and only if the request was admitted.
  /// For all calls to AdmitQuery(), ReleaseQuery() should also be called after
  /// the query completes to ensure that the pool statistics are updated.
  Status AdmitQuery(QuerySchedule* schedule);

  /// Updates the pool statistics when a query completes (either successfully,
  /// is cancelled or failed). This should be called for all requests that have
  /// been submitted via AdmitQuery(). (If the request was not admitted, this is
  /// a no-op.)
  /// This does not block.
  void ReleaseQuery(const QuerySchedule& schedule);

  /// Registers the request queue topic with the statestore.
  Status Init();

 private:
  class PoolStats;
  friend class PoolStats;

  /// Subscription manager used to handle admission control updates. This is not
  /// owned by this class.
  StatestoreSubscriber* subscriber_;

  /// Used for user-to-pool resolution and looking up pool configurations. Not owned by
  /// the AdmissionController.
  RequestPoolService* request_pool_service_;

  /// Metrics subsystem access
  MetricGroup* metrics_group_;

  /// Thread dequeuing and admitting queries.
  std::unique_ptr<Thread> dequeue_thread_;

  // The local impalad's host/port id, used to construct topic keys.
  const std::string host_id_;

  /// Serializes/deserializes TPoolStats when sending and receiving topic updates.
  ThriftSerializer thrift_serializer_;

  /// Protects all access to all variables below.
  /// Coordinates access to the results of the promise QueueNode::is_admitted,
  /// but the lock is not required to wait on the promise.
  boost::mutex admission_ctrl_lock_;

  /// Maps from host id to memory reserved and memory admitted, both aggregates over all
  /// pools. See the class doc for a definition of reserved and admitted. Protected by
  /// admission_ctrl_lock_.
  typedef boost::unordered_map<std::string, int64_t> HostMemMap;
  HostMemMap host_mem_reserved_;
  HostMemMap host_mem_admitted_;

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
    };

    PoolStats(AdmissionController* parent, const std::string& name)
      : name_(name), parent_(parent), agg_num_running_(0), agg_num_queued_(0),
        agg_mem_reserved_(0), local_mem_admitted_(0) {
      InitMetrics();
    }

    int64_t agg_num_running() const { return agg_num_running_; }
    int64_t agg_num_queued() const { return agg_num_queued_; }
    int64_t EffectiveMemReserved() const {
      return std::max(agg_mem_reserved_, local_mem_admitted_);
    }

    /// ADMISSION LIFECYCLE METHODS
    /// The following methods update the pool stats when the request represented by
    /// schedule is admitted, released, queued, or dequeued.
    void Admit(const QuerySchedule& schedule);
    void Release(const QuerySchedule& schedule);
    void Queue(const QuerySchedule& schedule);
    void Dequeue(const QuerySchedule& schedule, bool timed_out);


    /// STATESTORE CALLBACK METHODS
    /// Updates the local_stats_.mem_reserved with the pool mem tracker. Called
    /// before sending local_stats().
    void UpdateMemTrackerStats();

    /// Called on a full topic update to clear all stats before processing the update.
    void ClearRemoteStats() { remote_stats_.clear(); }

    /// Called to update remote host TPoolStats with the new host_stats for the
    /// specified host. If host_stats is NULL the stats for the specified remote host
    /// are removed (i.e. topic deletion).
    void UpdateRemoteStats(const std::string& backend_id, TPoolStats* host_stats);

    /// Called after updating local_stats_ and remote_stats_ to update the aggregate
    /// values of agg_num_running_, agg_num_queued_, and agg_mem_reserved_. The in/out
    /// parameter host_mem_reserved is a map from host id to memory reserved used to
    /// aggregate the mem reserved values across all pools for each host. Used by
    /// UpdateClusterAggregates() to update host_mem_reserved_; it provides the host
    /// aggregates when called over all pools.
    void UpdateAggregates(HostMemMap* host_mem_reserved);

    const TPoolStats& local_stats() { return local_stats_; }

    /// Updates the metrics exposing the pool configuration to those in pool_cfg.
    void UpdateConfigMetrics(const TPoolConfig& pool_cfg);

    PoolMetrics* metrics() { return &metrics_; }
    std::string DebugString() const;
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
    void InitMetrics();
  };

  /// Map of pool names to pool stats. Accessed via GetPoolStats().
  /// Protected by admission_ctrl_lock_.
  typedef boost::unordered_map<std::string, PoolStats> PoolStatsMap;
  PoolStatsMap pool_stats_;

  /// The set of pools that have changed between topic updates that need stats to be sent
  /// to the statestore. The key is the pool name.
  typedef boost::unordered_set<std::string> PoolSet;
  PoolSet pools_for_updates_;

  /// Structure stored in a QueryQueue representing a request. This struct lives only
  /// during the call to AdmitQuery().
  struct QueueNode : public InternalQueue<QueueNode>::Node {
    QueueNode(const QuerySchedule& query_schedule) : schedule(query_schedule) { }

    /// Set when the request is admitted or rejected by the dequeuing thread. Used
    /// by AdmitQuery() to wait for admission or until the timeout is reached.
    /// The admission_ctrl_lock_ is not held while waiting on this promise, but
    /// the lock should be held when checking the result because the dequeuing
    /// thread holds it to Set().
    Promise<bool> is_admitted;

    /// The query schedule of the queued request. The schedule lives longer than the
    /// duration of the QueueNode, which only lives the duration of the call to
    /// AdmitQuery.
    const QuerySchedule& schedule;
  };

  /// Queue for the queries waiting to be admitted for execution. Once the
  /// maximum number of concurrently executing queries has been reached,
  /// incoming queries are queued and admitted FCFS.
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

  /// Re-computes the per-pool aggregate stats and the per-host aggregates in
  /// host_mem_reserved_ using each pool's remote_stats_ and local_stats_.
  /// Called by UpdatePoolStats() after handling updates and deletions.
  /// Must hold admission_ctrl_lock_.
  void UpdateClusterAggregates();

  /// Dequeues and admits queued queries when notified by dequeue_cv_.
  void DequeueLoop();

  /// Returns true if schedule can be admitted to the pool with pool_cfg.
  /// admit_from_queue is true if attempting to admit from the queue. Otherwise, returns
  /// false and not_admitted_reason specifies why the request can not be admitted
  /// immediately. Caller owns not_admitted_reason.  Must hold admission_ctrl_lock_.
  bool CanAdmitRequest(const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
      bool admit_from_queue, std::string* not_admitted_reason);

  /// Returns true if there is enough memory available to admit the query based on the
  /// schedule, the aggregate pool memory, and the per-host memory. If not, this returns
  /// false and returns the reason in mem_unavailable_reason. Caller owns
  /// mem_unavailable_reason. Must hold admission_ctrl_lock_.
  bool HasAvailableMemResources(const QuerySchedule& schedule,
      const TPoolConfig& pool_cfg, std::string* mem_unavailable_reason);

  /// Adds per_node_mem to host_mem_admitted_ for each host in schedule. Must hold
  /// admission_ctrl_lock_.
  void UpdateHostMemAdmitted(const QuerySchedule& schedule, int64_t per_node_mem);

  /// Returns true if this request must be rejected immediately, e.g. requires more
  /// memory than possible to reserve or the queue is already full. If true,
  /// rejection_reason is set to a explanation of why the request was rejected.
  /// Must hold admission_ctrl_lock_.
  bool RejectImmediately(QuerySchedule* schedule, const TPoolConfig& pool_cfg,
      std::string* rejection_reason);

  /// Gets or creates the PoolStats for pool_name. Must hold admission_ctrl_lock_.
  PoolStats* GetPoolStats(const std::string& pool_name);
};

}

#endif // SCHEDULING_ADMISSION_CONTROLLER_H
