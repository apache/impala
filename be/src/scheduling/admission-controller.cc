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

#include "scheduling/admission-controller.h"

#include <boost/algorithm/string.hpp>
#include <boost/mem_fn.hpp>
#include <gutil/strings/stringpiece.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/executor-group.h"
#include "scheduling/query-schedule.h"
#include "scheduling/scheduler.h"
#include "service/impala-server.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"

#include "common/names.h"

using namespace strings;

DEFINE_int64(queue_wait_timeout_ms, 60 * 1000, "Maximum amount of time (in "
    "milliseconds) that a request will wait to be admitted before timing out.");

// The stale topic warning threshold is made configurable to allow suppressing the
// error if it turns out to be noisy on some deployments or allow lowering the
// threshold to help debug admission control issues. Hidden so that we have the
// option of making this a no-op later.
DEFINE_int64_hidden(admission_control_stale_topic_threshold_ms, 5 * 1000,
    "Threshold above which the admission controller will append warnings to "
    "error messages and profiles warning that the admission control topic is "
    "stale so that the admission control decision may have been based on stale "
    "state data. The default, 5 seconds, is chosen to minimise false positives but "
    "capture most cases where the Impala daemon is disconnected from the statestore "
    "or topic updates are seriously delayed.");

namespace impala {

const int64_t AdmissionController::PoolStats::HISTOGRAM_NUM_OF_BINS = 128;
const int64_t AdmissionController::PoolStats::HISTOGRAM_BIN_SIZE = 1024L * 1024L * 1024L;
const double AdmissionController::PoolStats::EMA_MULTIPLIER = 0.2;

/// Convenience method.
string PrintBytes(int64_t value) {
  return PrettyPrinter::Print(value, TUnit::BYTES);
}

// Delimiter used for topic keys of the form "<pool_name><delimiter><backend_id>".
// "!" is used because the backend id contains a colon, but it should not contain "!".
// When parsing the topic key we need to be careful to find the last instance in
// case the pool name contains it as well.
const char TOPIC_KEY_DELIMITER = '!';

// Delimiter used for the resource pool prefix of executor groups. In order to be used for
// queries in "resource-pool-A", an executor group name must start with
// "resource-pool-A-".
const char POOL_GROUP_DELIMITER = '-';

const string EXEC_GROUP_QUERY_LOAD_KEY_FORMAT =
  "admission-controller.executor-group.num-queries-executing.$0";

// Define metric key format strings for metrics in PoolMetrics
// '$0' is replaced with the pool name by strings::Substitute
const string TOTAL_ADMITTED_METRIC_KEY_FORMAT =
  "admission-controller.total-admitted.$0";
const string TOTAL_QUEUED_METRIC_KEY_FORMAT =
  "admission-controller.total-queued.$0";
const string TOTAL_DEQUEUED_METRIC_KEY_FORMAT =
  "admission-controller.total-dequeued.$0";
const string TOTAL_REJECTED_METRIC_KEY_FORMAT =
  "admission-controller.total-rejected.$0";
const string TOTAL_TIMED_OUT_METRIC_KEY_FORMAT =
  "admission-controller.total-timed-out.$0";
const string TOTAL_RELEASED_METRIC_KEY_FORMAT =
  "admission-controller.total-released.$0";
const string TIME_IN_QUEUE_METRIC_KEY_FORMAT =
  "admission-controller.time-in-queue-ms.$0";
const string AGG_NUM_RUNNING_METRIC_KEY_FORMAT =
  "admission-controller.agg-num-running.$0";
const string AGG_NUM_QUEUED_METRIC_KEY_FORMAT =
  "admission-controller.agg-num-queued.$0";
const string AGG_MEM_RESERVED_METRIC_KEY_FORMAT =
  "admission-controller.agg-mem-reserved.$0";
const string LOCAL_MEM_ADMITTED_METRIC_KEY_FORMAT =
  "admission-controller.local-mem-admitted.$0";
const string LOCAL_NUM_ADMITTED_RUNNING_METRIC_KEY_FORMAT =
  "admission-controller.local-num-admitted-running.$0";
const string LOCAL_NUM_QUEUED_METRIC_KEY_FORMAT =
  "admission-controller.local-num-queued.$0";
const string LOCAL_BACKEND_MEM_USAGE_METRIC_KEY_FORMAT =
  "admission-controller.local-backend-mem-usage.$0";
const string LOCAL_BACKEND_MEM_RESERVED_METRIC_KEY_FORMAT =
  "admission-controller.local-backend-mem-reserved.$0";
const string POOL_MAX_MEM_RESOURCES_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-mem-resources.$0";
const string POOL_MAX_REQUESTS_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-requests.$0";
const string POOL_MAX_QUEUED_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-queued.$0";
const string POOL_MAX_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-query-mem-limit.$0";
const string POOL_MIN_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-min-query-mem-limit.$0";
const string POOL_CLAMP_MEM_LIMIT_QUERY_OPTION_METRIC_KEY_FORMAT =
  "admission-controller.pool-clamp-mem-limit-query-option.$0";
const string POOL_MAX_RUNNING_QUERIES_MULTIPLE_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-running-queries-multiple.$0";
const string POOL_MAX_QUEUED_QUERIES_MULTIPLE_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-queued-queries-multiple.$0";
const string POOL_MAX_MEMORY_MULTIPLE_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-memory-multiple.$0";
const string POOL_MAX_RUNNING_QUERIES_DERIVED_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-running-queries-derived.$0";
const string POOL_MAX_QUEUED_QUERIES_DERIVED_METRIC_KEY_FORMAT =
  "admission-controller.max-queued-queries-derived.$0";
const string POOL_MAX_MEMORY_DERIVED_METRIC_KEY_FORMAT =
  "admission-controller.max-memory-derived.$0";

// Profile query events
const string QUERY_EVENT_SUBMIT_FOR_ADMISSION = "Submit for admission";
const string QUERY_EVENT_QUEUED = "Queued";
const string QUERY_EVENT_COMPLETED_ADMISSION = "Completed admission";

// Profile info strings
const string AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT = "Admission result";
const string AdmissionController::PROFILE_INFO_VAL_ADMIT_IMMEDIATELY =
    "Admitted immediately";
const string AdmissionController::PROFILE_INFO_VAL_QUEUED = "Queued";
const string AdmissionController::PROFILE_INFO_VAL_CANCELLED_IN_QUEUE =
    "Cancelled (queued)";
const string AdmissionController::PROFILE_INFO_VAL_ADMIT_QUEUED = "Admitted (queued)";
const string AdmissionController::PROFILE_INFO_VAL_REJECTED = "Rejected";
const string AdmissionController::PROFILE_INFO_VAL_TIME_OUT = "Timed out (queued)";
const string AdmissionController::PROFILE_INFO_KEY_INITIAL_QUEUE_REASON =
    "Initial admission queue reason";
const string AdmissionController::PROFILE_INFO_VAL_INITIAL_QUEUE_REASON =
    "waited $0 ms, reason: $1";
const string AdmissionController::PROFILE_INFO_KEY_LAST_QUEUED_REASON =
    "Latest admission queue reason";
const string AdmissionController::PROFILE_INFO_KEY_ADMITTED_MEM =
    "Cluster Memory Admitted";
const string AdmissionController::PROFILE_INFO_KEY_EXECUTOR_GROUP = "Executor Group";
const string AdmissionController::PROFILE_INFO_KEY_STALENESS_WARNING =
    "Admission control state staleness";
const string AdmissionController::PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME =
    "AdmissionControlTimeSinceLastUpdate";

// Error status string details
const string REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM_FIXED =
    "Invalid pool config: the min_query_mem_limit $0 is greater than the "
    "max_mem_resources $1 (configured statically)";
const string REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM_MULTIPLE =
    "The min_query_mem_limit $0 is greater than the current max_mem_resources $1 ($2); "
    "queries will not be admitted until more executors are available.";
const string REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_LIMIT =
    "Invalid pool config: the min_query_mem_limit is greater than the "
    "max_query_mem_limit ($0 > $1)";
const string REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation is greater than memory available to the query for buffer "
    "reservations. Memory reservation needed given the current plan: $0. Adjust either "
    "the mem_limit or the pool config (max-query-mem-limit, min-query-mem-limit) for the "
    "query to allow the query memory limit to be at least $1. Note that changing the "
    "mem_limit may also change the plan. See the query profile for more information "
    "about the per-node memory requirements.";
const string REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation on backend '$0' is greater than memory available to the "
    "query for buffer reservations. Increase the buffer_pool_limit to $1. See the query "
    "profile for more information about the per-node memory requirements.";
const string REASON_MIN_RESERVATION_OVER_POOL_MEM =
    "minimum memory reservation needed is greater than pool max mem resources. Pool "
    "max mem resources: $0 ($1). Cluster-wide memory reservation needed: $2. Increase "
    "the pool max mem resources. See the query profile for more information about the "
    "per-node memory requirements.";
const string REASON_DISABLED_MAX_MEM_RESOURCES =
    "disabled by pool max mem resources set to 0";
const string REASON_DISABLED_REQUESTS_LIMIT = "disabled by requests limit set to 0";
// $2 is the description of how the queue limit was calculated, $3 is the staleness
// detail.
const string REASON_QUEUE_FULL = "queue full, limit=$0 ($1), num_queued=$2.$3";
const string REASON_REQ_OVER_POOL_MEM =
    "request memory needed $0 is greater than pool max mem resources $1 ($2).\n\n"
    "Use the MEM_LIMIT query option to indicate how much memory is required per node. "
    "The total memory needed is the per-node MEM_LIMIT times the number of nodes "
    "executing the query. See the Admission Control documentation for more information.";
const string REASON_REQ_OVER_NODE_MEM =
    "request memory needed $0 is greater than memory available for admission $1 "
    "of $2.\n\nUse the MEM_LIMIT query option to indicate how much memory is required "
    "per node.";
const string REASON_THREAD_RESERVATION_LIMIT_EXCEEDED =
    "thread reservation on backend '$0' is greater than the THREAD_RESERVATION_LIMIT "
    "query option value: $1 > $2.";
const string REASON_THREAD_RESERVATION_AGG_LIMIT_EXCEEDED =
    "sum of thread reservations across all $0 backends is greater than the "
    "THREAD_RESERVATION_AGGREGATE_LIMIT query option value: $1 > $2.";
// $0 is the error message returned by the scheduler.
const string REASON_SCHEDULER_ERROR = "Error during scheduling: $0";
const string REASON_LOCAL_BACKEND_NOT_STARTED = "Local backend has not started up yet.";
const string REASON_NO_EXECUTOR_GROUPS = "Waiting for executors to start. Only DDL "
    "queries can currently run.";

// Queue decision details
// $0 = num running queries, $1 = num queries limit, $2 = num queries limit explanation,
// $3 = staleness detail
const string QUEUED_NUM_RUNNING =
    "number of running queries $0 is at or over limit $1 ($2)$3.";
// $0 = queue size, $1 = staleness detail
const string QUEUED_QUEUE_NOT_EMPTY = "queue is not empty (size $0); queued queries are "
    "executed first.$1";
// $0 = pool name, $1 = pool max memory, $2 = pool max memory explanation,
// $3 = pool mem needed, $4 = pool mem available, $5 = staleness detail
const string POOL_MEM_NOT_AVAILABLE =
    "Not enough aggregate memory available in pool $0 "
    "with max mem resources $1 ($2). Needed $3 but only $4 was available.$5";
// $0 = host name, $1 = host mem needed, $3 = host mem available, $4 = staleness detail
const string HOST_MEM_NOT_AVAILABLE = "Not enough memory available on host $0. "
    "Needed $1 but only $2 out of $3 was available.$4";

// $0 = host name, $1 = num admitted, $2 = max requests
const string HOST_SLOT_NOT_AVAILABLE = "No query slot available on host $0. "
                                       "$1/$2 are already admitted.";

// Parses the pool name and backend_id from the topic key if it is valid.
// Returns true if the topic key is valid and pool_name and backend_id are set.
static inline bool ParsePoolTopicKey(const string& topic_key, string* pool_name,
    string* backend_id) {
  // Topic keys will look something like: poolname!hostname:22000
  // The '!' delimiter should always be present, the pool name must be
  // at least 1 character, and network address must be at least 3 characters (including
  // ':' and if the hostname and port are each only 1 character). Then the topic_key must
  // be at least 5 characters (1 + 1 + 3).
  const int MIN_TOPIC_KEY_SIZE = 5;
  if (topic_key.length() < MIN_TOPIC_KEY_SIZE) {
    VLOG_QUERY << "Invalid topic key for pool: " << topic_key;
    return false;
  }

  size_t pos = topic_key.find_last_of(TOPIC_KEY_DELIMITER);
  if (pos == string::npos || pos >= topic_key.size() - 1) {
    VLOG_QUERY << "Invalid topic key for pool: " << topic_key;
    return false;
  }
  *pool_name = topic_key.substr(0, pos);
  *backend_id = topic_key.substr(pos + 1);
  return true;
}

// Return a debug string for the pool stats.
static string DebugPoolStats(const TPoolStats& stats) {
  stringstream ss;
  ss << "num_admitted_running=" << stats.num_admitted_running << ", ";
  ss << "num_queued=" << stats.num_queued << ", ";
  ss << "backend_mem_reserved=" << PrintBytes(stats.backend_mem_reserved);
  return ss.str();
}

string AdmissionController::PoolStats::DebugString() const {
  stringstream ss;
  ss << "agg_num_running=" << agg_num_running_ << ", ";
  ss << "agg_num_queued=" << agg_num_queued_ << ", ";
  ss << "agg_mem_reserved=" << PrintBytes(agg_mem_reserved_) << ", ";
  ss << " local_host(local_mem_admitted=" << PrintBytes(local_mem_admitted_) << ", ";
  ss << DebugPoolStats(local_stats_) << ")";
  return ss.str();
}

// TODO: do we need host_id_ to come from host_addr or can it just take the same id
// the Scheduler has (coming from the StatestoreSubscriber)?
AdmissionController::AdmissionController(ClusterMembershipMgr* cluster_membership_mgr,
    StatestoreSubscriber* subscriber, RequestPoolService* request_pool_service,
    MetricGroup* metrics, const TNetworkAddress& host_addr)
  : cluster_membership_mgr_(cluster_membership_mgr),
    subscriber_(subscriber),
    request_pool_service_(request_pool_service),
    metrics_group_(metrics->GetOrCreateChildGroup("admission-controller")),
    host_id_(TNetworkAddressToString(host_addr)),
    thrift_serializer_(false),
    done_(false) {
  cluster_membership_mgr_->RegisterUpdateCallbackFn(
      [this](ClusterMembershipMgr::SnapshotPtr snapshot) {
        this->UpdateExecGroupMetricMap(snapshot);
      });
}

AdmissionController::~AdmissionController() {
  // If the dequeue thread is not running (e.g. if Init() fails), then there is
  // nothing to do.
  if (dequeue_thread_ == nullptr) return;

  // The AdmissionController should live for the lifetime of the impalad, but
  // for unit tests we need to ensure that no thread is waiting on the
  // condition variable. This notifies the dequeue thread to stop and waits
  // for it to finish.
  {
    // Lock to ensure the dequeue thread will see the update to done_
    lock_guard<mutex> l(admission_ctrl_lock_);
    done_ = true;
    dequeue_cv_.NotifyOne();
  }
  dequeue_thread_->Join();
}

Status AdmissionController::Init() {
  RETURN_IF_ERROR(Thread::Create("scheduling", "admission-thread",
      &AdmissionController::DequeueLoop, this, &dequeue_thread_));
  auto cb = [this](
      const StatestoreSubscriber::TopicDeltaMap& state,
      vector<TTopicDelta>* topic_updates) { UpdatePoolStats(state, topic_updates); };
  Status status = subscriber_->AddTopic(Statestore::IMPALA_REQUEST_QUEUE_TOPIC,
      /* is_transient=*/true, /* populate_min_subscriber_topic_version=*/false,
      /* filter_prefix=*/"", cb);
  if (!status.ok()) {
    status.AddDetail("AdmissionController failed to register request queue topic");
  }
  return status;
}

void AdmissionController::PoolStats::AdmitQueryAndMemory(const QuerySchedule& schedule) {
  int64_t cluster_mem_admitted = schedule.GetClusterMemoryToAdmit();
  DCHECK_GT(cluster_mem_admitted, 0);
  local_mem_admitted_ += cluster_mem_admitted;
  metrics_.local_mem_admitted->Increment(cluster_mem_admitted);

  agg_num_running_ += 1;
  metrics_.agg_num_running->Increment(1L);

  local_stats_.num_admitted_running += 1;
  metrics_.local_num_admitted_running->Increment(1L);

  metrics_.total_admitted->Increment(1L);
}

void AdmissionController::PoolStats::ReleaseQuery(int64_t peak_mem_consumption) {
  // Update stats tracking the number of running and admitted queries.
  agg_num_running_ -= 1;
  metrics_.agg_num_running->Increment(-1L);

  local_stats_.num_admitted_running -= 1;
  metrics_.local_num_admitted_running->Increment(-1L);

  metrics_.total_released->Increment(1L);
  DCHECK_GE(local_stats_.num_admitted_running, 0);
  DCHECK_GE(agg_num_running_, 0);

  // Update the 'peak_mem_histogram' based on the given peak memory consumption of the
  // query.
  int64_t histogram_bucket =
      BitUtil::RoundUp(peak_mem_consumption, HISTOGRAM_BIN_SIZE) / HISTOGRAM_BIN_SIZE;
  histogram_bucket = std::max(std::min(histogram_bucket, HISTOGRAM_NUM_OF_BINS), 1L) - 1;
  peak_mem_histogram_[histogram_bucket] = ++(peak_mem_histogram_[histogram_bucket]);
}

void AdmissionController::PoolStats::ReleaseMem(int64_t mem_to_release) {
  // Update stats tracking memory admitted.
  DCHECK_GT(mem_to_release, 0);
  local_mem_admitted_ -= mem_to_release;
  DCHECK_GE(local_mem_admitted_, 0);
  metrics_.local_mem_admitted->Increment(-mem_to_release);
}

void AdmissionController::PoolStats::Queue() {
  agg_num_queued_ += 1;
  metrics_.agg_num_queued->Increment(1L);

  local_stats_.num_queued += 1;
  metrics_.local_num_queued->Increment(1L);

  metrics_.total_queued->Increment(1L);
}

void AdmissionController::PoolStats::Dequeue(bool timed_out) {
  agg_num_queued_ -= 1;
  metrics_.agg_num_queued->Increment(-1L);

  local_stats_.num_queued -= 1;
  metrics_.local_num_queued->Increment(-1L);

  DCHECK_GE(agg_num_queued_, 0);
  DCHECK_GE(local_stats_.num_queued, 0);
  if (timed_out) {
    metrics_.total_timed_out->Increment(1L);
  } else {
    metrics_.total_dequeued->Increment(1L);
  }
}

void AdmissionController::UpdateStatsOnReleaseForBackends(
    const QuerySchedule& schedule, const vector<TNetworkAddress>& host_addrs) {
  int64_t total_mem_to_release = 0;
  for (auto host_addr : host_addrs) {
    auto backend_exec_params = schedule.per_backend_exec_params().find(host_addr);
    if (backend_exec_params == schedule.per_backend_exec_params().end()) {
      string err_msg =
          strings::Substitute("Error: Cannot find exec params of host $0 for query $1.",
              PrintThrift(host_addr), PrintId(schedule.query_id()));
      DCHECK(false) << err_msg;
      LOG(ERROR) << err_msg;
      continue;
    }
    int64_t mem_to_release = GetMemToAdmit(schedule, backend_exec_params->second);
    UpdateHostStats(host_addr, -mem_to_release, -1);
    total_mem_to_release += mem_to_release;
  }
  PoolStats* pool_stats = GetPoolStats(schedule);
  pool_stats->ReleaseMem(total_mem_to_release);
  pools_for_updates_.insert(schedule.request_pool());
}

void AdmissionController::UpdateStatsOnAdmission(const QuerySchedule& schedule) {
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host_addr = entry.first;
    int64_t mem_to_admit = GetMemToAdmit(schedule, entry.second);
    UpdateHostStats(host_addr, mem_to_admit, 1);
  }
  PoolStats* pool_stats = GetPoolStats(schedule);
  pool_stats->AdmitQueryAndMemory(schedule);
  pools_for_updates_.insert(schedule.request_pool());
}

void AdmissionController::UpdateHostStats(
    const TNetworkAddress& host_addr, int64_t mem_to_admit, int num_queries_to_admit) {
  const string host = TNetworkAddressToString(host_addr);
  VLOG_ROW << "Update admitted mem reserved for host=" << host
           << " prev=" << PrintBytes(host_stats_[host].mem_admitted)
           << " new=" << PrintBytes(host_stats_[host].mem_admitted + mem_to_admit);
  host_stats_[host].mem_admitted += mem_to_admit;
  DCHECK_GE(host_stats_[host].mem_admitted, 0);
  VLOG_ROW << "Update admitted queries for host=" << host
           << " prev=" << host_stats_[host].num_admitted
           << " new=" << host_stats_[host].num_admitted + num_queries_to_admit;
  host_stats_[host].num_admitted += num_queries_to_admit;
  DCHECK_GE(host_stats_[host].num_admitted, 0);
}

// Helper method used by CanAccommodateMaxInitialReservation(). Returns true if the given
// 'mem_limit' can accommodate 'buffer_reservation'. If not, returns false and the
// details about the memory shortage in 'mem_unavailable_reason'.
static bool CanMemLimitAccommodateReservation(
    int64_t mem_limit, int64_t buffer_reservation, string* mem_unavailable_reason) {
  if (mem_limit <= 0) return true; // No mem limit.
  const int64_t max_reservation =
      ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
  if (buffer_reservation <= max_reservation) return true;
  const int64_t required_mem_limit =
      ReservationUtil::GetMinMemLimitFromReservation(buffer_reservation);
  *mem_unavailable_reason = Substitute(REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION,
      PrintBytes(buffer_reservation), PrintBytes(required_mem_limit));
  return false;
}

bool AdmissionController::CanAccommodateMaxInitialReservation(
    const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
    string* mem_unavailable_reason) {
  const int64_t executor_mem_limit = schedule.per_backend_mem_limit();
  const int64_t executor_min_reservation = schedule.largest_min_reservation();
  const int64_t coord_mem_limit = schedule.coord_backend_mem_limit();
  const int64_t coord_min_reservation = schedule.coord_min_reservation();
  return CanMemLimitAccommodateReservation(
             executor_mem_limit, executor_min_reservation, mem_unavailable_reason)
      && CanMemLimitAccommodateReservation(
             coord_mem_limit, coord_min_reservation, mem_unavailable_reason);
}

bool AdmissionController::HasAvailableMemResources(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, int64_t cluster_size, string* mem_unavailable_reason) {
  const string& pool_name = schedule.request_pool();
  const int64_t pool_max_mem = GetMaxMemForPool(pool_cfg, cluster_size);
  // If the pool doesn't have memory resources configured, always true.
  if (pool_max_mem < 0) return true;

  // Otherwise, two conditions must be met:
  // 1) The memory estimated to be reserved by all queries in this pool *plus* the total
  //    memory needed for this query must be within the max pool memory resources
  //    specified.
  // 2) Each individual backend must have enough mem available within its process limit
  //    to execute the query.

  // Case 1:
  PoolStats* stats = GetPoolStats(schedule);
  int64_t cluster_mem_to_admit = schedule.GetClusterMemoryToAdmit();
  VLOG_RPC << "Checking agg mem in pool=" << pool_name << " : " << stats->DebugString()
           << " executor_group=" << schedule.executor_group()
           << " cluster_mem_needed=" << PrintBytes(cluster_mem_to_admit)
           << " pool_max_mem=" << PrintBytes(pool_max_mem) << " ("
           << GetMaxMemForPoolDescription(pool_cfg, cluster_size) << ")";
  if (stats->EffectiveMemReserved() + cluster_mem_to_admit > pool_max_mem) {
    *mem_unavailable_reason = Substitute(POOL_MEM_NOT_AVAILABLE, pool_name,
        PrintBytes(pool_max_mem), GetMaxMemForPoolDescription(pool_cfg, cluster_size),
        PrintBytes(cluster_mem_to_admit),
        PrintBytes(max(pool_max_mem - stats->EffectiveMemReserved(), 0L)),
        GetStalenessDetailLocked(" "));
    return false;
  }

  // Case 2:
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host = entry.first;
    const string host_id = TNetworkAddressToString(host);
    int64_t admit_mem_limit = entry.second.be_desc.admit_mem_limit;
    const HostStats& host_stats = host_stats_[host_id];
    int64_t mem_reserved = host_stats.mem_reserved;
    int64_t mem_admitted = host_stats.mem_admitted;
    int64_t mem_to_admit = GetMemToAdmit(schedule, entry.second);
    VLOG_ROW << "Checking memory on host=" << host_id
             << " mem_reserved=" << PrintBytes(mem_reserved)
             << " mem_admitted=" << PrintBytes(mem_admitted)
             << " needs=" << PrintBytes(mem_to_admit)
             << " admit_mem_limit=" << PrintBytes(admit_mem_limit);
    int64_t effective_host_mem_reserved = std::max(mem_reserved, mem_admitted);
    if (effective_host_mem_reserved + mem_to_admit > admit_mem_limit) {
      *mem_unavailable_reason =
          Substitute(HOST_MEM_NOT_AVAILABLE, host_id, PrintBytes(mem_to_admit),
              PrintBytes(max(admit_mem_limit - effective_host_mem_reserved, 0L)),
              PrintBytes(admit_mem_limit), GetStalenessDetailLocked(" "));
      return false;
    }
  }
  const TQueryOptions& query_opts = schedule.query_options();
  if (!query_opts.__isset.buffer_pool_limit || query_opts.buffer_pool_limit <= 0) {
    // Check if a change in pool_cfg.max_query_mem_limit (while the query was queued)
    // resulted in a decrease in the computed per_host_mem_limit such that it can no
    // longer accommodate the largest min_reservation.
    return CanAccommodateMaxInitialReservation(
        schedule, pool_cfg, mem_unavailable_reason);
  }
  return true;
}

bool AdmissionController::HasAvailableSlot(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, string* unavailable_reason) {
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host = entry.first;
    const string host_id = TNetworkAddressToString(host);
    int64_t admit_num_queries_limit = entry.second.be_desc.admit_num_queries_limit;
    int64_t num_admitted = host_stats_[host_id].num_admitted;
    VLOG_ROW << "Checking available slot on host=" << host_id
             << " num_admitted=" << num_admitted << " needs=" << num_admitted + 1
             << " admit_num_queries_limit=" << admit_num_queries_limit;
    if (num_admitted >= admit_num_queries_limit) {
      *unavailable_reason =
          Substitute(HOST_SLOT_NOT_AVAILABLE, host_id, num_admitted,
              admit_num_queries_limit);
      return false;
    }
  }
  return true;
}

bool AdmissionController::CanAdmitRequest(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, int64_t cluster_size, bool admit_from_queue,
    string* not_admitted_reason) {
  // Can't admit if:
  //  (a) There are already queued requests (and this is not admitting from the queue).
  //  (b) The resource pool is already at the maximum number of requests.
  //  (c) One of the executors in 'schedule' is already at its maximum number of requests
  //      (when not using the default executor group).
  //  (d) There are not enough memory resources available for the query.

  const int64_t max_requests = GetMaxRequestsForPool(pool_cfg, cluster_size);
  PoolStats* pool_stats = GetPoolStats(schedule);
  bool default_group =
      schedule.executor_group() == ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME;
  if (!admit_from_queue && pool_stats->local_stats().num_queued > 0) {
    *not_admitted_reason = Substitute(QUEUED_QUEUE_NOT_EMPTY,
        pool_stats->local_stats().num_queued, GetStalenessDetailLocked(" "));
    return false;
  }
  if (max_requests >= 0 && pool_stats->agg_num_running() >= max_requests) {
    // All executor groups are limited by the aggregate number of queries running in the
    // pool.
    *not_admitted_reason = Substitute(QUEUED_NUM_RUNNING, pool_stats->agg_num_running(),
        max_requests, GetMaxRequestsForPoolDescription(pool_cfg, cluster_size),
        GetStalenessDetailLocked(" "));
    return false;
  }
  if (!default_group && !HasAvailableSlot(schedule, pool_cfg, not_admitted_reason)) {
    // All non-default executor groups are also limited by the number of running queries
    // per executor.
    // TODO(IMPALA-8757): Extend slot based admission to default executor group
    return false;
  }
  if (!HasAvailableMemResources(schedule, pool_cfg, cluster_size, not_admitted_reason)) {
    return false;
  }
  return true;
}

bool AdmissionController::RejectForCluster(const string& pool_name,
    const TPoolConfig& pool_cfg, bool admit_from_queue, int64_t cluster_size,
    string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());

  // Checks related to pool max_requests:
  if (GetMaxRequestsForPool(pool_cfg, cluster_size) == 0) {
    *rejection_reason = REASON_DISABLED_REQUESTS_LIMIT;
    return true;
  }

  // Checks related to pool max_mem_resources:
  int64_t max_mem = GetMaxMemForPool(pool_cfg, cluster_size);
  if (max_mem == 0) {
    *rejection_reason = REASON_DISABLED_MAX_MEM_RESOURCES;
    return true;
  }

  if (max_mem > 0 && pool_cfg.min_query_mem_limit > max_mem) {
    if (PoolHasFixedMemoryLimit(pool_cfg)) {
      *rejection_reason = Substitute(REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM_FIXED,
          pool_cfg.min_query_mem_limit, max_mem);
    } else {
      *rejection_reason =
          Substitute(REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM_MULTIPLE,
              pool_cfg.min_query_mem_limit, max_mem,
              GetMaxMemForPoolDescription(pool_cfg, cluster_size));
    }
    return true;
  }

  if (pool_cfg.max_query_mem_limit > 0
      && pool_cfg.min_query_mem_limit > pool_cfg.max_query_mem_limit) {
    *rejection_reason = Substitute(REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_LIMIT,
        pool_cfg.min_query_mem_limit, pool_cfg.max_query_mem_limit);
    return true;
  }

  PoolStats* stats = GetPoolStats(pool_name);
  int64_t max_queued = GetMaxQueuedForPool(pool_cfg, cluster_size);
  if (!admit_from_queue && stats->agg_num_queued() >= max_queued) {
    *rejection_reason = Substitute(REASON_QUEUE_FULL, max_queued,
        GetMaxQueuedForPoolDescription(pool_cfg, cluster_size), stats->agg_num_queued(),
        GetStalenessDetailLocked(" "));
    return true;
  }

  return false;
}

bool AdmissionController::RejectForSchedule(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, int64_t cluster_size, int64_t group_size,
    string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());

  // Compute the max (over all backends), the cluster totals (across all backends) for
  // min_mem_reservation_bytes, thread_reservation, the min admit_mem_limit
  // (over all executors) and the admit_mem_limit of the coordinator.
  pair<const TNetworkAddress*, int64_t> largest_min_mem_reservation(nullptr, -1);
  int64_t cluster_min_mem_reservation_bytes = 0;
  pair<const TNetworkAddress*, int64_t> max_thread_reservation(nullptr, 0);
  pair<const TNetworkAddress*, int64_t> min_executor_admit_mem_limit(
      nullptr, std::numeric_limits<int64_t>::max());
  pair<const TNetworkAddress*, int64_t> coord_admit_mem_limit(
      nullptr, std::numeric_limits<int64_t>::max());
  int64_t cluster_thread_reservation = 0;
  for (const auto& e : schedule.per_backend_exec_params()) {
    const BackendExecParams& bp = e.second;
    cluster_min_mem_reservation_bytes += bp.min_mem_reservation_bytes;
    if (bp.min_mem_reservation_bytes > largest_min_mem_reservation.second) {
      largest_min_mem_reservation = make_pair(&e.first, bp.min_mem_reservation_bytes);
    }
    cluster_thread_reservation += bp.thread_reservation;
    if (bp.thread_reservation > max_thread_reservation.second) {
      max_thread_reservation = make_pair(&e.first, bp.thread_reservation);
    }
    if (bp.is_coord_backend) {
      coord_admit_mem_limit.first = &e.first;
      coord_admit_mem_limit.second = bp.be_desc.admit_mem_limit;
    } else if (bp.be_desc.admit_mem_limit < min_executor_admit_mem_limit.second) {
      min_executor_admit_mem_limit.first = &e.first;
      min_executor_admit_mem_limit.second = bp.be_desc.admit_mem_limit;
    }
  }

  // Checks related to the min buffer reservation against configured query memory limits:
  const TQueryOptions& query_opts = schedule.query_options();
  if (query_opts.__isset.buffer_pool_limit && query_opts.buffer_pool_limit > 0) {
    if (largest_min_mem_reservation.second > query_opts.buffer_pool_limit) {
      *rejection_reason = Substitute(REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION,
          TNetworkAddressToString(*largest_min_mem_reservation.first),
          PrintBytes(largest_min_mem_reservation.second));
      return true;
    }
  } else if (!CanAccommodateMaxInitialReservation(schedule, pool_cfg, rejection_reason)) {
    // If buffer_pool_limit is not explicitly set, it's calculated from mem_limit.
    return true;
  }

  // Check thread reservation limits.
  if (query_opts.__isset.thread_reservation_limit
      && query_opts.thread_reservation_limit > 0
      && max_thread_reservation.second > query_opts.thread_reservation_limit) {
    *rejection_reason = Substitute(REASON_THREAD_RESERVATION_LIMIT_EXCEEDED,
        TNetworkAddressToString(*max_thread_reservation.first),
        max_thread_reservation.second, query_opts.thread_reservation_limit);
    return true;
  }
  if (query_opts.__isset.thread_reservation_aggregate_limit
      && query_opts.thread_reservation_aggregate_limit > 0
      && cluster_thread_reservation > query_opts.thread_reservation_aggregate_limit) {
    *rejection_reason = Substitute(REASON_THREAD_RESERVATION_AGG_LIMIT_EXCEEDED,
        schedule.per_backend_exec_params().size(), cluster_thread_reservation,
        query_opts.thread_reservation_aggregate_limit);
    return true;
  }

  // Checks related to pool max_mem_resources:
  // We perform these checks here against the group_size to prevent queuing up queries
  // that would never be able to reserve the required memory on an executor group.
  int64_t max_mem = GetMaxMemForPool(pool_cfg, group_size);
  if (max_mem == 0) {
    *rejection_reason = REASON_DISABLED_MAX_MEM_RESOURCES;
    return true;
  }
  if (max_mem > 0) {
    if (cluster_min_mem_reservation_bytes > max_mem) {
      *rejection_reason = Substitute(REASON_MIN_RESERVATION_OVER_POOL_MEM,
          PrintBytes(max_mem), GetMaxMemForPoolDescription(pool_cfg, group_size),
          PrintBytes(cluster_min_mem_reservation_bytes));
      return true;
    }
    int64_t cluster_mem_to_admit = schedule.GetClusterMemoryToAdmit();
    if (cluster_mem_to_admit > max_mem) {
      *rejection_reason =
          Substitute(REASON_REQ_OVER_POOL_MEM, PrintBytes(cluster_mem_to_admit),
              PrintBytes(max_mem), GetMaxMemForPoolDescription(pool_cfg, group_size));
      return true;
    }
    int64_t executor_mem_to_admit = schedule.per_backend_mem_to_admit();
    VLOG_ROW << "Checking executor mem with executor_mem_to_admit = "
             << executor_mem_to_admit
             << " and min_admit_mem_limit.second = "
             << min_executor_admit_mem_limit.second;
    if (executor_mem_to_admit > min_executor_admit_mem_limit.second) {
      *rejection_reason =
          Substitute(REASON_REQ_OVER_NODE_MEM, PrintBytes(executor_mem_to_admit),
              PrintBytes(min_executor_admit_mem_limit.second),
              TNetworkAddressToString(*min_executor_admit_mem_limit.first));
      return true;
    }
    int64_t coord_mem_to_admit = schedule.coord_backend_mem_to_admit();
    VLOG_ROW << "Checking coordinator mem with coord_mem_to_admit = "
             << coord_mem_to_admit
             << " and coord_admit_mem_limit.second = " << coord_admit_mem_limit.second;
    if (coord_mem_to_admit > coord_admit_mem_limit.second) {
      *rejection_reason = Substitute(REASON_REQ_OVER_NODE_MEM,
          PrintBytes(coord_mem_to_admit), PrintBytes(coord_admit_mem_limit.second),
          TNetworkAddressToString(*coord_admit_mem_limit.first));
      return true;
    }
  }
  return false;
}

void AdmissionController::PoolStats::UpdateConfigMetrics(
    const TPoolConfig& pool_cfg, int64_t cluster_size) {
  metrics_.pool_max_mem_resources->SetValue(pool_cfg.max_mem_resources);
  metrics_.pool_max_requests->SetValue(pool_cfg.max_requests);
  metrics_.pool_max_queued->SetValue(pool_cfg.max_queued);
  metrics_.max_query_mem_limit->SetValue(pool_cfg.max_query_mem_limit);
  metrics_.min_query_mem_limit->SetValue(pool_cfg.min_query_mem_limit);
  metrics_.clamp_mem_limit_query_option->SetValue(pool_cfg.clamp_mem_limit_query_option);
  metrics_.max_running_queries_multiple->SetValue(pool_cfg.max_running_queries_multiple);
  metrics_.max_queued_queries_multiple->SetValue(pool_cfg.max_queued_queries_multiple);
  metrics_.max_memory_multiple->SetValue(pool_cfg.max_memory_multiple);
}

void AdmissionController::PoolStats::UpdateDerivedMetrics(
    const TPoolConfig& pool_cfg, int64_t cluster_size) {
  metrics_.max_running_queries_derived->SetValue(
      GetMaxRequestsForPool(pool_cfg, cluster_size));
  metrics_.max_queued_queries_derived->SetValue(
      GetMaxQueuedForPool(pool_cfg, cluster_size));
  metrics_.max_memory_derived->SetValue(GetMaxMemForPool(pool_cfg, cluster_size));
}

Status AdmissionController::SubmitForAdmission(const AdmissionRequest& request,
    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admit_outcome,
    std::unique_ptr<QuerySchedule>* schedule_result) {
  DCHECK(schedule_result->get() == nullptr);

  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr_->GetSnapshot();
  DCHECK(membership_snapshot.get() != nullptr);
  string blacklist_str = membership_snapshot->executor_blacklist.BlacklistToString();
  if (!blacklist_str.empty()) {
    request.summary_profile->AddInfoString("Blacklisted Executors", blacklist_str);
  }

  // Note the queue_node will not exist in the queue when this method returns.
  QueueNode queue_node(request, admit_outcome, request.summary_profile);

  // Re-resolve the pool name to propagate any resolution errors now that this request is
  // known to require a valid pool. All executor groups / schedules will use the same pool
  // name.
  string pool_name;
  TPoolConfig pool_cfg;
  RETURN_IF_ERROR(
      ResolvePoolAndGetConfig(request.request.query_ctx, &pool_name, &pool_cfg));
  request.summary_profile->AddInfoString("Request Pool", pool_name);

  const int64_t cluster_size = GetClusterSize(*membership_snapshot);
  // We track this outside of the queue node so that it is still available after the query
  // has been dequeued.
  string initial_queue_reason;
  ScopedEvent completedEvent(request.query_events, QUERY_EVENT_COMPLETED_ADMISSION);
  {
    // Take lock to ensure the Dequeue thread does not modify the request queue.
    lock_guard<mutex> lock(admission_ctrl_lock_);
    request.query_events->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);

    pool_config_map_[pool_name] = pool_cfg;
    PoolStats* stats = GetPoolStats(pool_name);
    stats->UpdateConfigMetrics(pool_cfg, cluster_size);
    stats->UpdateDerivedMetrics(pool_cfg, cluster_size);

    bool must_reject = !FindGroupToAdmitOrReject(cluster_size, membership_snapshot,
        pool_cfg, /* admit_from_queue=*/false, stats, &queue_node);
    if (must_reject) {
      AdmissionOutcome outcome = admit_outcome->Set(AdmissionOutcome::REJECTED);
      if (outcome != AdmissionOutcome::REJECTED) {
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
        VLOG_QUERY << "Ready to be " << PROFILE_INFO_VAL_REJECTED
                   << " but already cancelled, query id=" << PrintId(request.query_id);
        return Status::CANCELLED;
      }
      request.summary_profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_REJECTED);
      stats->metrics()->total_rejected->Increment(1);
      const ErrorMsg& rejected_msg = ErrorMsg(
          TErrorCode::ADMISSION_REJECTED, pool_name, queue_node.not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    }

    if (queue_node.admitted_schedule.get() != nullptr) {
      const string& group_name = queue_node.admitted_schedule->executor_group();
      VLOG(3) << "Can admit to group " << group_name << " (or cancelled)";
      DCHECK_EQ(stats->local_stats().num_queued, 0);
      *schedule_result = std::move(queue_node.admitted_schedule);
      AdmissionOutcome outcome = admit_outcome->Set(AdmissionOutcome::ADMITTED);
      if (outcome != AdmissionOutcome::ADMITTED) {
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
        VLOG_QUERY << "Ready to be " << PROFILE_INFO_VAL_ADMIT_IMMEDIATELY
                   << " but already cancelled, query id=" << PrintId(request.query_id);
        return Status::CANCELLED;
      }
      VLOG_QUERY << "Admitting query id=" << PrintId(request.query_id);
      AdmitQuery(schedule_result->get(), false);
      stats->UpdateWaitTime(0);
      VLOG_RPC << "Final: " << stats->DebugString();
      return Status::OK();
    }

    // We cannot immediately admit but do not need to reject, so queue the request
    RequestQueue* queue = &request_queue_map_[pool_name];
    VLOG_QUERY << "Queuing, query id=" << PrintId(request.query_id)
               << " reason: " << queue_node.not_admitted_reason;
    initial_queue_reason = queue_node.not_admitted_reason;
    stats->Queue();
    queue->Enqueue(&queue_node);
  }

  // Update the profile info before waiting. These properties will be updated with
  // their final state after being dequeued.
  request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_QUEUED);
  request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_INITIAL_QUEUE_REASON, initial_queue_reason);
  request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_LAST_QUEUED_REASON, queue_node.not_admitted_reason);
  request.query_events->MarkEvent(QUERY_EVENT_QUEUED);

  int64_t queue_wait_timeout_ms = FLAGS_queue_wait_timeout_ms;
  if (pool_cfg.__isset.queue_timeout_ms) {
    queue_wait_timeout_ms = pool_cfg.queue_timeout_ms;
  }
  queue_wait_timeout_ms = max<int64_t>(0, queue_wait_timeout_ms);
  int64_t wait_start_ms = MonotonicMillis();

  // Block in Get() up to the time out, waiting for the promise to be set when the query
  // is admitted or cancelled.
  bool timed_out;
  admit_outcome->Get(queue_wait_timeout_ms, &timed_out);
  int64_t wait_time_ms = MonotonicMillis() - wait_start_ms;
  request.summary_profile->AddInfoString(PROFILE_INFO_KEY_INITIAL_QUEUE_REASON,
      Substitute(
          PROFILE_INFO_VAL_INITIAL_QUEUE_REASON, wait_time_ms, initial_queue_reason));

  // Disallow the FAIL action here. It would leave the queue in an inconsistent state.
  DebugActionNoFail(request.query_options, "AC_AFTER_ADMISSION_OUTCOME");

  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    // If the query has not been admitted or cancelled up till now, it will be considered
    // to be timed out.
    AdmissionOutcome outcome = admit_outcome->Set(AdmissionOutcome::TIMED_OUT);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pools_for_updates_.insert(pool_name);
    PoolStats* pool_stats = GetPoolStats(pool_name);
    pool_stats->UpdateWaitTime(wait_time_ms);
    if (outcome == AdmissionOutcome::REJECTED) {
      if (queue->Remove(&queue_node)) pool_stats->Dequeue(true);
      request.summary_profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_REJECTED);
      const ErrorMsg& rejected_msg = ErrorMsg(
          TErrorCode::ADMISSION_REJECTED, pool_name, queue_node.not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    } else if (outcome == AdmissionOutcome::TIMED_OUT) {
      bool removed = queue->Remove(&queue_node);
      DCHECK(removed);
      pool_stats->Dequeue(true);
      request.summary_profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_TIME_OUT);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_TIMED_OUT,
          queue_wait_timeout_ms, pool_name, queue_node.not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    } else if (outcome == AdmissionOutcome::CANCELLED) {
      if (queue->Remove(&queue_node)) pool_stats->Dequeue(false);
      request.summary_profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_CANCELLED_IN_QUEUE);
      VLOG_QUERY << PROFILE_INFO_VAL_CANCELLED_IN_QUEUE
                 << ", query id=" << PrintId(request.query_id);
      return Status::CANCELLED;
    }
    // The dequeue thread updates the stats (to avoid a race condition) so we do
    // not change them here.
    DCHECK_ENUM_EQ(outcome, AdmissionOutcome::ADMITTED);
    DCHECK(queue_node.admitted_schedule.get() != nullptr);
    *schedule_result = std::move(queue_node.admitted_schedule);
    DCHECK(!queue->Contains(&queue_node));
    VLOG_QUERY << "Admitted queued query id=" << PrintId(request.query_id);
    VLOG_RPC << "Final: " << pool_stats->DebugString();
    return Status::OK();
  }
}

void AdmissionController::ReleaseQuery(
    const QuerySchedule& schedule, int64_t peak_mem_consumption) {
  const string& pool_name = schedule.request_pool();
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    DCHECK_EQ(num_released_backends_.at(schedule.query_id()), 0);
    num_released_backends_.erase(num_released_backends_.find(schedule.query_id()));
    PoolStats* stats = GetPoolStats(schedule);
    stats->ReleaseQuery(peak_mem_consumption);
    // No need to update the Host Stats as they should have been updated in
    // ReleaseQueryBackends.
    pools_for_updates_.insert(pool_name);
    UpdateExecGroupMetric(schedule.executor_group(), -1);
    VLOG_RPC << "Released query id=" << PrintId(schedule.query_id()) << " "
             << stats->DebugString();
  }
  dequeue_cv_.NotifyOne();
}

void AdmissionController::ReleaseQueryBackends(
    const QuerySchedule& schedule, const vector<TNetworkAddress>& host_addrs) {
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    UpdateStatsOnReleaseForBackends(schedule, host_addrs);

    // Update num_released_backends_.
    auto released_backends = num_released_backends_.find(schedule.query_id());
    if (released_backends != num_released_backends_.end()) {
      released_backends->second -= host_addrs.size();
    } else {
      string err_msg = Substitute("Unable to find num released backends for query $0",
          PrintId(schedule.query_id()));
      DCHECK(false) << err_msg;
      LOG(ERROR) << err_msg;
    }

    if (VLOG_IS_ON(2)) {
      stringstream ss;
      ss << "Released query backend(s) ";
      for (auto host_addr : host_addrs) ss << PrintThrift(host_addr) << " ";
      ss << "for query id=" << PrintId(schedule.query_id()) << " "
         << GetPoolStats(schedule)->DebugString();
      VLOG(2) << ss.str();
    }
  }
  dequeue_cv_.NotifyOne();
}

Status AdmissionController::ResolvePoolAndGetConfig(
    const TQueryCtx& query_ctx, string* pool_name, TPoolConfig* pool_config) {
  RETURN_IF_ERROR(request_pool_service_->ResolveRequestPool(query_ctx, pool_name));
  DCHECK_EQ(query_ctx.request_pool, *pool_name);
  return request_pool_service_->GetPoolConfig(*pool_name, pool_config);
}

// Statestore subscriber callback for IMPALA_REQUEST_QUEUE_TOPIC.
void AdmissionController::UpdatePoolStats(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    AddPoolUpdates(subscriber_topic_updates);

    StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
        incoming_topic_deltas.find(Statestore::IMPALA_REQUEST_QUEUE_TOPIC);
    if (topic != incoming_topic_deltas.end()) {
      const TTopicDelta& delta = topic->second;
      // Delta and non-delta updates are handled the same way, except for a full update
      // we first clear the backend TPoolStats. We then update the global map
      // and then re-compute the pool stats for any pools that changed.
      if (!delta.is_delta) {
        VLOG_ROW << "Full impala-request-queue stats update";
        for (auto& entry : pool_stats_) entry.second.ClearRemoteStats();
      }
      HandleTopicUpdates(delta.topic_entries);
    }
    UpdateClusterAggregates();
    last_topic_update_time_ms_ = MonotonicMillis();
  }
  dequeue_cv_.NotifyOne(); // Dequeue and admit queries on the dequeue thread
}

void AdmissionController::PoolStats::UpdateRemoteStats(const string& host_id,
    TPoolStats* host_stats) {
  DCHECK_NE(host_id, parent_->host_id_); // Shouldn't be updating for local host.
  RemoteStatsMap::iterator it = remote_stats_.find(host_id);
  if (VLOG_ROW_IS_ON) {
    stringstream ss;
    ss << "Stats update for pool=" << name_ << " backend=" << host_id;
    if (host_stats == nullptr) ss << " topic deletion";
    if (it != remote_stats_.end()) ss << " previous: " << DebugPoolStats(it->second);
    if (host_stats != nullptr) ss << " new: " << DebugPoolStats(*host_stats);
    VLOG_ROW << ss.str();
  }
  if (host_stats == nullptr) {
    if (it != remote_stats_.end()) {
      remote_stats_.erase(it);
    } else {
      VLOG_QUERY << "Attempted to remove non-existent remote stats for host=" << host_id;
    }
  } else {
    remote_stats_[host_id] = *host_stats;
  }
}

void AdmissionController::HandleTopicUpdates(const vector<TTopicItem>& topic_updates) {
  for (const TTopicItem& item: topic_updates) {
    string pool_name;
    string topic_backend_id;
    if (!ParsePoolTopicKey(item.key, &pool_name, &topic_backend_id)) continue;
    // The topic entry from this subscriber is handled specially; the stats coming
    // from the statestore are likely already outdated.
    if (topic_backend_id == host_id_) continue;
    if (item.deleted) {
      GetPoolStats(pool_name)->UpdateRemoteStats(topic_backend_id, nullptr);
      continue;
    }
    TPoolStats remote_update;
    uint32_t len = item.value.size();
    Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &remote_update);
    if (!status.ok()) {
      VLOG_QUERY << "Error deserializing pool update with key: " << item.key;
      continue;
    }
    GetPoolStats(pool_name)->UpdateRemoteStats(topic_backend_id, &remote_update);
  }
}

void AdmissionController::PoolStats::UpdateAggregates(HostMemMap* host_mem_reserved) {
  const string& coord_id = parent_->host_id_;
  int64_t num_running = 0;
  int64_t num_queued = 0;
  int64_t mem_reserved = 0;
  for (const PoolStats::RemoteStatsMap::value_type& remote_entry : remote_stats_) {
    const string& host = remote_entry.first;
    // Skip an update from this subscriber as the information may be outdated.
    // The stats from this coordinator will be added below.
    if (host == coord_id) continue;
    const TPoolStats& remote_pool_stats = remote_entry.second;
    DCHECK_GE(remote_pool_stats.num_admitted_running, 0);
    DCHECK_GE(remote_pool_stats.num_queued, 0);
    DCHECK_GE(remote_pool_stats.backend_mem_reserved, 0);
    num_running += remote_pool_stats.num_admitted_running;
    num_queued += remote_pool_stats.num_queued;

    // Update the per-pool and per-host aggregates with the mem reserved by this host in
    // this pool.
    mem_reserved += remote_pool_stats.backend_mem_reserved;
    (*host_mem_reserved)[host] += remote_pool_stats.backend_mem_reserved;
    // TODO(IMPALA-8762): For multiple coordinators, need to track the number of running
    // queries per executor, i.e. every admission controller needs to send the full map to
    // everyone else.
  }
  num_running += local_stats_.num_admitted_running;
  num_queued += local_stats_.num_queued;
  mem_reserved += local_stats_.backend_mem_reserved;
  (*host_mem_reserved)[coord_id] += local_stats_.backend_mem_reserved;

  DCHECK_GE(num_running, 0);
  DCHECK_GE(num_queued, 0);
  DCHECK_GE(mem_reserved, 0);
  DCHECK_GE(num_running, local_stats_.num_admitted_running);
  DCHECK_GE(num_queued, local_stats_.num_queued);

  if (agg_num_running_ == num_running && agg_num_queued_ == num_queued
      && agg_mem_reserved_ == mem_reserved) {
    DCHECK_EQ(num_running, metrics_.agg_num_running->GetValue());
    DCHECK_EQ(num_queued, metrics_.agg_num_queued->GetValue());
    DCHECK_EQ(mem_reserved, metrics_.agg_mem_reserved->GetValue());
    return;
  }
  VLOG_ROW << "Recomputed agg stats, previous: " << DebugString();
  agg_num_running_ = num_running;
  agg_num_queued_ = num_queued;
  agg_mem_reserved_ = mem_reserved;
  metrics_.agg_num_running->SetValue(num_running);
  metrics_.agg_num_queued->SetValue(num_queued);
  metrics_.agg_mem_reserved->SetValue(mem_reserved);
  VLOG_ROW << "Updated: " << DebugString();
}

void AdmissionController::UpdateClusterAggregates() {
  // Recompute mem_reserved for all hosts.
  PoolStats::HostMemMap updated_mem_reserved;
  for (auto& entry : pool_stats_) entry.second.UpdateAggregates(&updated_mem_reserved);

  stringstream ss;
  ss << "Updated mem reserved for hosts:";
  int i = 0;
  for (const auto& e : updated_mem_reserved) {
    int64_t old_mem_reserved = host_stats_[e.first].mem_reserved;
    if (old_mem_reserved == e.second) continue;
    host_stats_[e.first].mem_reserved = e.second;
    if (VLOG_ROW_IS_ON) {
      ss << endl << e.first << ": " << PrintBytes(old_mem_reserved);
      ss << " -> " << PrintBytes(e.second);
      ++i;
    }
  }
  if (i > 0) VLOG_ROW << ss.str();
}

Status AdmissionController::ComputeGroupSchedules(
    ClusterMembershipMgr::SnapshotPtr membership_snapshot, QueueNode* queue_node) {
  int64_t previous_membership_version = 0;
  if (queue_node->membership_snapshot.get() != nullptr) {
    previous_membership_version = queue_node->membership_snapshot->version;
  }
  int64_t current_membership_version = membership_snapshot->version;

  DCHECK_GT(current_membership_version, 0);
  DCHECK_GE(current_membership_version, previous_membership_version);
  if (current_membership_version <= previous_membership_version) {
    VLOG(3) << "No rescheduling necessary, previous membership version: "
            << previous_membership_version
            << ", current membership version: " << current_membership_version;
    return Status::OK();
  }
  const AdmissionRequest& request = queue_node->admission_request;
  VLOG(3) << "Scheduling query " << PrintId(request.query_id)
          << " with membership version " << current_membership_version;

  queue_node->membership_snapshot = membership_snapshot;
  std::vector<GroupSchedule>* output_schedules = &queue_node->group_schedules;
  output_schedules->clear();

  const string& pool_name = request.request.query_ctx.request_pool;

  // If the first statestore update arrives before the local backend has finished starting
  // up, we might not have a local backend descriptor yet. We return no schedules, which
  // will result in the query being queued.
  if (membership_snapshot->local_be_desc == nullptr) {
    queue_node->not_admitted_reason = REASON_LOCAL_BACKEND_NOT_STARTED;
    LOG(WARNING) << queue_node->not_admitted_reason;
    return Status::OK();
  }
  const TBackendDescriptor& local_be_desc = *membership_snapshot->local_be_desc;

  vector<const ExecutorGroup*> executor_groups;
  GetExecutorGroupsForPool(
      membership_snapshot->executor_groups, pool_name, &executor_groups);

  if (executor_groups.empty()) {
    queue_node->not_admitted_reason = REASON_NO_EXECUTOR_GROUPS;
    LOG(WARNING) << queue_node->not_admitted_reason;
    return Status::OK();
  }

  // We loop over the executor groups in a deterministic order. This means we will fill up
  // each executor group before considering an unused one. In particular, we will not try
  // to balance queries across executor groups equally.
  // TODO(IMPALA-8731): balance queries across executor groups more evenly
  for (const ExecutorGroup* executor_group : executor_groups) {
    DCHECK(executor_group->IsHealthy());
    DCHECK_GT(executor_group->NumExecutors(), 0);
    unique_ptr<QuerySchedule> group_schedule =
        make_unique<QuerySchedule>(request.query_id, request.request,
            request.query_options, request.summary_profile, request.query_events);
    const string& group_name = executor_group->name();
    VLOG(3) << "Scheduling for executor group: " << group_name << " with "
            << executor_group->NumExecutors() << " executors";
    const Scheduler::ExecutorConfig group_config = {*executor_group, local_be_desc};
    RETURN_IF_ERROR(ExecEnv::GetInstance()->scheduler()->Schedule(
        group_config, group_schedule.get()));
    DCHECK(!group_schedule->executor_group().empty());
    output_schedules->emplace_back(std::move(group_schedule), *executor_group);
  }
  DCHECK(!output_schedules->empty());
  return Status::OK();
}

bool AdmissionController::FindGroupToAdmitOrReject(int64_t cluster_size,
    ClusterMembershipMgr::SnapshotPtr membership_snapshot, const TPoolConfig& pool_config,
    bool admit_from_queue, PoolStats* pool_stats, QueueNode* queue_node) {
  // Check for rejection based on current cluster size
  const string& pool_name = pool_stats->name();
  string rejection_reason;
  if (RejectForCluster(
          pool_name, pool_config, admit_from_queue, cluster_size, &rejection_reason)) {
    DCHECK(!rejection_reason.empty());
    queue_node->not_admitted_reason = rejection_reason;
    return false;
  }

  // Compute schedules
  Status ret = ComputeGroupSchedules(membership_snapshot, queue_node);
  if (!ret.ok()) {
    DCHECK(queue_node->not_admitted_reason.empty());
    queue_node->not_admitted_reason = Substitute(REASON_SCHEDULER_ERROR, ret.GetDetail());
    return false;
  }
  if (queue_node->group_schedules.empty()) {
    DCHECK(!queue_node->not_admitted_reason.empty());
    return true;
  }

  for (GroupSchedule& group_schedule : queue_node->group_schedules) {
    const ExecutorGroup& executor_group = group_schedule.executor_group;
    DCHECK_GT(executor_group.NumExecutors(), 0);
    QuerySchedule* schedule = group_schedule.schedule.get();
    schedule->UpdateMemoryRequirements(pool_config);

    const string& group_name = executor_group.name();
    int64_t group_size = executor_group.NumExecutors();
    VLOG(3) << "Trying to admit query to pool " << pool_name << " in executor group "
            << group_name << " (" << group_size << " executors)";

    const int64_t max_queued = GetMaxQueuedForPool(pool_config, cluster_size);
    const int64_t max_mem = GetMaxMemForPool(pool_config, cluster_size);
    const int64_t max_requests = GetMaxRequestsForPool(pool_config, cluster_size);
    VLOG_QUERY << "Trying to admit id=" << PrintId(schedule->query_id())
               << " in pool_name=" << pool_name << " executor_group_name=" << group_name
               << " per_host_mem_estimate="
               << PrintBytes(schedule->GetPerExecutorMemoryEstimate())
               << " dedicated_coord_mem_estimate="
               << PrintBytes(schedule->GetDedicatedCoordMemoryEstimate())
               << " max_requests=" << max_requests << " ("
               << GetMaxRequestsForPoolDescription(pool_config, cluster_size) << ")"
               << " max_queued=" << max_queued << " ("
               << GetMaxQueuedForPoolDescription(pool_config, cluster_size) << ")"
               << " max_mem=" << PrintBytes(max_mem) << " ("
               << GetMaxMemForPoolDescription(pool_config, cluster_size) << ")";
    VLOG_QUERY << "Stats: " << pool_stats->DebugString();

    // Query is rejected if the rejection check fails on *any* group.
    if (RejectForSchedule(
            *schedule, pool_config, cluster_size, group_size, &rejection_reason)) {
      DCHECK(!rejection_reason.empty());
      queue_node->not_admitted_reason = rejection_reason;
      return false;
    }

    if (CanAdmitRequest(*schedule, pool_config, cluster_size, admit_from_queue,
            &queue_node->not_admitted_reason)) {
      queue_node->admitted_schedule = std::move(group_schedule.schedule);
      return true;
    } else {
      VLOG_RPC << "Cannot admit query " << queue_node->admission_request.query_id
               << " to group " << group_name << ": " << queue_node->not_admitted_reason;
    }
  }
  return true;
}

void AdmissionController::PoolStats::UpdateMemTrackerStats() {
  // May be NULL if no queries have ever executed in this pool on this node but another
  // node sent stats for this pool.
  MemTracker* tracker =
      ExecEnv::GetInstance()->pool_mem_trackers()->GetRequestPoolMemTracker(name_, false);

  const int64_t current_reserved =
      tracker == nullptr ? static_cast<int64_t>(0) : tracker->GetPoolMemReserved();
  if (current_reserved != local_stats_.backend_mem_reserved) {
    parent_->pools_for_updates_.insert(name_);
    local_stats_.backend_mem_reserved = current_reserved;
    metrics_.local_backend_mem_reserved->SetValue(current_reserved);
  }

  const int64_t current_usage =
      tracker == nullptr ? static_cast<int64_t>(0) : tracker->consumption();
  metrics_.local_backend_mem_usage->SetValue(current_usage);
}

void AdmissionController::AddPoolUpdates(vector<TTopicDelta>* topic_updates) {
  // local_stats_ are updated eagerly except for backend_mem_reserved (which isn't used
  // for local admission control decisions). Update that now before sending local_stats_.
  for (auto& entry : pool_stats_) {
    entry.second.UpdateMemTrackerStats();
  }
  if (pools_for_updates_.empty()) return;
  topic_updates->push_back(TTopicDelta());
  TTopicDelta& topic_delta = topic_updates->back();
  topic_delta.topic_name = Statestore::IMPALA_REQUEST_QUEUE_TOPIC;
  for (const string& pool_name: pools_for_updates_) {
    DCHECK(pool_stats_.find(pool_name) != pool_stats_.end());
    PoolStats* stats = GetPoolStats(pool_name);
    VLOG_ROW << "Sending topic update " << stats->DebugString();
    topic_delta.topic_entries.push_back(TTopicItem());
    TTopicItem& topic_item = topic_delta.topic_entries.back();
    topic_item.key = MakePoolTopicKey(pool_name, host_id_);
    Status status = thrift_serializer_.SerializeToString(&stats->local_stats(),
        &topic_item.value);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to serialize query pool stats: " << status.GetDetail();
      topic_updates->pop_back();
    }
  }
  pools_for_updates_.clear();
}

void AdmissionController::DequeueLoop() {
  while (true) {
    unique_lock<mutex> lock(admission_ctrl_lock_);
    if (done_) break;
    dequeue_cv_.Wait(lock);
    ClusterMembershipMgr::SnapshotPtr membership_snapshot =
        cluster_membership_mgr_->GetSnapshot();

    // If a query was queued while the cluster is still starting up but the client facing
    // services have already started to accept connections, the whole membership can still
    // be empty.
    if (membership_snapshot->executor_groups.empty()) continue;
    const int64_t cluster_size = GetClusterSize(*membership_snapshot);

    for (const PoolConfigMap::value_type& entry: pool_config_map_) {
      const string& pool_name = entry.first;
      const TPoolConfig& pool_config = entry.second;
      PoolStats* stats = GetPoolStats(pool_name, /* dcheck_exists=*/true);
      stats->UpdateDerivedMetrics(pool_config, cluster_size);

      if (stats->local_stats().num_queued == 0) continue; // Nothing to dequeue
      DCHECK_GE(stats->agg_num_queued(), stats->local_stats().num_queued);

      RequestQueue& queue = request_queue_map_[pool_name];
      int64_t max_to_dequeue = GetMaxToDequeue(queue, stats, pool_config, cluster_size);
      VLOG_RPC << "Dequeue thread will try to admit " << max_to_dequeue << " requests"
               << ", pool=" << pool_name
               << ", num_queued=" << stats->local_stats().num_queued
               << " cluster_size=" << cluster_size;
      if (max_to_dequeue == 0) continue; // to next pool.

      while (max_to_dequeue > 0 && !queue.empty()) {
        QueueNode* queue_node = queue.head();
        DCHECK(queue_node != nullptr);
        // Find a group that can admit the query
        bool is_cancelled = queue_node->admit_outcome->IsSet()
            && queue_node->admit_outcome->Get() == AdmissionOutcome::CANCELLED;

        bool is_rejected = !is_cancelled
            && !FindGroupToAdmitOrReject(cluster_size, membership_snapshot, pool_config,
                   /* admit_from_queue=*/true, stats, queue_node);

        if (!is_cancelled && !is_rejected
            && queue_node->admitted_schedule.get() == nullptr) {
          // If no group was found, stop trying to dequeue.
          // TODO(IMPALA-2968): Requests further in the queue may be blocked
          // unnecessarily. Consider a better policy once we have better test scenarios.
          LogDequeueFailed(queue_node, queue_node->not_admitted_reason);
          break;
        }

        // At this point we know that the query must be taken off the queue
        queue.Dequeue();
        --max_to_dequeue;
        VLOG(3) << "Dequeueing from stats for pool " << pool_name;
        stats->Dequeue(false);

        if (is_rejected) {
          AdmissionOutcome outcome =
              queue_node->admit_outcome->Set(AdmissionOutcome::REJECTED);
          if (outcome == AdmissionOutcome::REJECTED) {
            stats->metrics()->total_rejected->Increment(1);
            continue; // next query
          } else {
            DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
            is_cancelled = true;
          }
        }
        DCHECK(is_cancelled || queue_node->admitted_schedule != nullptr);

        const TUniqueId& query_id = queue_node->admission_request.query_id;
        if (!is_cancelled) {
          VLOG_QUERY << "Admitting from queue: query=" << PrintId(query_id);
          AdmissionOutcome outcome =
              queue_node->admit_outcome->Set(AdmissionOutcome::ADMITTED);
          if (outcome != AdmissionOutcome::ADMITTED) {
            DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
            is_cancelled = true;
          }
        }

        if (is_cancelled) {
          VLOG_QUERY << "Dequeued cancelled query=" << PrintId(query_id);
          continue; // next query
        }

        DCHECK(queue_node->admit_outcome->IsSet());
        DCHECK_ENUM_EQ(queue_node->admit_outcome->Get(), AdmissionOutcome::ADMITTED);
        DCHECK(!is_cancelled);
        DCHECK(!is_rejected);
        DCHECK(queue_node->admitted_schedule != nullptr);
        AdmitQuery(queue_node->admitted_schedule.get(), true);
      }
      pools_for_updates_.insert(pool_name);
    }
  }
}

int64_t AdmissionController::GetMaxToDequeue(RequestQueue& queue, PoolStats* stats,
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (PoolLimitsRunningQueriesCount(pool_config)) {
    const int64_t max_requests = GetMaxRequestsForPool(pool_config, cluster_size);
    const int64_t total_available = max_requests - stats->agg_num_running();
    if (total_available <= 0) {
      // There is a limit for the number of running queries, so we can
      // see that nothing can run in this pool.
      // This can happen in the case of over-admission.
      if (!queue.empty()) {
        LogDequeueFailed(queue.head(),
            Substitute(QUEUED_NUM_RUNNING, stats->agg_num_running(), max_requests,
                GetMaxRequestsForPoolDescription(pool_config, cluster_size),
                GetStalenessDetailLocked(" ")));
      }
      return 0;
    }

    // Use the ratio of locally queued requests to agg queued so that each impalad
    // can dequeue a proportional amount total_available. Note, this offers no
    // fairness between impalads.
    double queue_size_ratio = static_cast<double>(stats->local_stats().num_queued)
        / static_cast<double>(max<int64_t>(1, stats->agg_num_queued()));
    DCHECK(queue_size_ratio <= 1.0);
    return min(stats->local_stats().num_queued,
        max<int64_t>(1, queue_size_ratio * total_available));
  } else {
    return stats->local_stats().num_queued; // No limit on num running requests
  }
}

void AdmissionController::LogDequeueFailed(QueueNode* node,
    const string& not_admitted_reason) {
  VLOG_QUERY << "Could not dequeue query id=" << PrintId(node->admission_request.query_id)
             << " reason: " << not_admitted_reason;
  node->admission_request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_LAST_QUEUED_REASON, not_admitted_reason);
}

AdmissionController::PoolStats* AdmissionController::GetPoolStats(
    const QuerySchedule& schedule) {
  DCHECK(!schedule.request_pool().empty());
  return GetPoolStats(schedule.request_pool());
}

AdmissionController::PoolStats* AdmissionController::GetPoolStats(
    const string& pool_name, bool dcheck_exists) {
  DCHECK(!pool_name.empty());
  auto it = pool_stats_.find(pool_name);
  DCHECK(!dcheck_exists || it != pool_stats_.end());
  if (it == pool_stats_.end()) {
    bool inserted;
    std::tie(it, inserted) = pool_stats_.emplace(pool_name, PoolStats(this, pool_name));
    DCHECK(inserted);
  }
  DCHECK(it != pool_stats_.end());
  return &it->second;
}

void AdmissionController::AdmitQuery(QuerySchedule* schedule, bool was_queued) {
  VLOG_RPC << "For Query " << PrintId(schedule->query_id())
           << " per_backend_mem_limit set to: "
           << PrintBytes(schedule->per_backend_mem_limit())
           << " per_backend_mem_to_admit set to: "
           << PrintBytes(schedule->per_backend_mem_to_admit())
           << " coord_backend_mem_limit set to: "
           << PrintBytes(schedule->coord_backend_mem_limit())
           << " coord_backend_mem_to_admit set to: "
           << PrintBytes(schedule->coord_backend_mem_to_admit());;
  // Update memory and number of queries.
  UpdateStatsOnAdmission(*schedule);
  UpdateExecGroupMetric(schedule->executor_group(), 1);
  // Update summary profile.
  const string& admission_result =
      was_queued ? PROFILE_INFO_VAL_ADMIT_QUEUED : PROFILE_INFO_VAL_ADMIT_IMMEDIATELY;
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, admission_result);
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMITTED_MEM, PrintBytes(schedule->GetClusterMemoryToAdmit()));
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_EXECUTOR_GROUP, schedule->executor_group());
  // We may have admitted based on stale information. Include a warning in the profile
  // if this this may be the case.
  int64_t time_since_update_ms;
  string staleness_detail = GetStalenessDetailLocked("", &time_since_update_ms);
  // IMPALA-8235: convert to TIME_NS because of issues with tools consuming TIME_MS.
  COUNTER_SET(ADD_COUNTER(schedule->summary_profile(),
      PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME, TUnit::TIME_NS),
      static_cast<int64_t>(time_since_update_ms * NANOS_PER_MICRO * MICROS_PER_MILLI));
  if (!staleness_detail.empty()) {
    schedule->summary_profile()->AddInfoString(
        PROFILE_INFO_KEY_STALENESS_WARNING, staleness_detail);
  }
  DCHECK(
      num_released_backends_.find(schedule->query_id()) == num_released_backends_.end());
  num_released_backends_[schedule->query_id()] =
      schedule->per_backend_exec_params().size();
}

string AdmissionController::GetStalenessDetail(const string& prefix,
    int64_t* ms_since_last_update) {
  lock_guard<mutex> lock(admission_ctrl_lock_);
  return GetStalenessDetailLocked(prefix, ms_since_last_update);
}

string AdmissionController::GetStalenessDetailLocked(const string& prefix,
    int64_t* ms_since_last_update) {
  int64_t ms_since_update = MonotonicMillis() - last_topic_update_time_ms_;
  if (ms_since_last_update != nullptr) *ms_since_last_update = ms_since_update;
  if (last_topic_update_time_ms_ == 0) {
    return Substitute("$0Warning: admission control information from statestore "
                      "is stale: no update has been received.", prefix);
  } else if (ms_since_update >= FLAGS_admission_control_stale_topic_threshold_ms) {
    return Substitute("$0Warning: admission control information from statestore "
                      "is stale: $1 since last update was received.",
        prefix, PrettyPrinter::Print(ms_since_update, TUnit::TIME_MS));
  }
  return "";
}

void AdmissionController::PoolToJsonLocked(const string& pool_name,
    rapidjson::Value* resource_pools, rapidjson::Document* document) {
  auto it = pool_stats_.find(pool_name);
  if (it == pool_stats_.end()) return;
  PoolStats* stats = &it->second;
  RequestQueue& queue = request_queue_map_[pool_name];
  // Get the pool stats
  using namespace rapidjson;
  Value pool_info_json(kObjectType);
  stats->ToJson(&pool_info_json, document);

  // Get the queued queries
  Value queued_queries(kArrayType);
  queue.Iterate([&queued_queries, document](QueueNode* node) {
    if (node->group_schedules.empty()) {
      Value query_info(kObjectType);
      query_info.AddMember("query_id", "N/A", document->GetAllocator());
      query_info.AddMember("mem_limit", 0, document->GetAllocator());
      query_info.AddMember("mem_limit_to_admit", 0, document->GetAllocator());
      query_info.AddMember("num_backends", 0, document->GetAllocator());
      queued_queries.PushBack(query_info, document->GetAllocator());
      return true;
    }
    QuerySchedule* schedule = node->group_schedules.begin()->schedule.get();
    Value query_info(kObjectType);
    Value query_id(PrintId(schedule->query_id()).c_str(), document->GetAllocator());
    query_info.AddMember("query_id", query_id, document->GetAllocator());
    query_info.AddMember(
        "mem_limit", schedule->per_backend_mem_limit(), document->GetAllocator());
    query_info.AddMember("mem_limit_to_admit", schedule->per_backend_mem_to_admit(),
        document->GetAllocator());
    query_info.AddMember("coord_mem_limit", schedule->coord_backend_mem_limit(),
        document->GetAllocator());
    query_info.AddMember("coord_mem_to_admit",
        schedule->coord_backend_mem_to_admit(), document->GetAllocator());
    query_info.AddMember("num_backends", schedule->per_backend_exec_params().size(),
        document->GetAllocator());
    queued_queries.PushBack(query_info, document->GetAllocator());
    return true;
  });
  pool_info_json.AddMember("queued_queries", queued_queries, document->GetAllocator());

  // Get the queued reason for the query at the head of the queue.
  if (!queue.empty()) {
    Value head_queued_reason(
        queue.head()
            ->profile->GetInfoString(PROFILE_INFO_KEY_LAST_QUEUED_REASON)
            ->c_str(),
        document->GetAllocator());
    pool_info_json.AddMember(
        "head_queued_reason", head_queued_reason, document->GetAllocator());
  }

  resource_pools->PushBack(pool_info_json, document->GetAllocator());
}

void AdmissionController::PoolToJson(const string& pool_name,
    rapidjson::Value* resource_pools, rapidjson::Document* document) {
  lock_guard<mutex> lock(admission_ctrl_lock_);
  PoolToJsonLocked(pool_name, resource_pools, document);
}

void AdmissionController::AllPoolsToJson(
    rapidjson::Value* resource_pools, rapidjson::Document* document) {
  lock_guard<mutex> lock(admission_ctrl_lock_);
  for (const PoolConfigMap::value_type& entry : pool_config_map_) {
    const string& pool_name = entry.first;
    PoolToJsonLocked(pool_name, resource_pools, document);
  }
}

void AdmissionController::PoolStats::UpdateWaitTime(int64_t wait_time_ms) {
  metrics()->time_in_queue_ms->Increment(wait_time_ms);
  if (wait_time_ms_ema_ == 0) {
    wait_time_ms_ema_ = wait_time_ms;
    return;
  }
  wait_time_ms_ema_ =
      wait_time_ms_ema_ * (1 - EMA_MULTIPLIER) + wait_time_ms * EMA_MULTIPLIER;
}

void AdmissionController::PoolStats::ToJson(
    rapidjson::Value* pool, rapidjson::Document* document) const {
  using namespace rapidjson;
  Value pool_name(name_.c_str(), document->GetAllocator());
  pool->AddMember("pool_name", pool_name, document->GetAllocator());
  pool->AddMember(
      "agg_num_running", metrics_.agg_num_running->GetValue(), document->GetAllocator());
  pool->AddMember(
      "agg_num_queued", metrics_.agg_num_queued->GetValue(), document->GetAllocator());
  pool->AddMember("agg_mem_reserved", metrics_.agg_mem_reserved->GetValue(),
      document->GetAllocator());
  pool->AddMember("local_mem_admitted", metrics_.local_mem_admitted->GetValue(),
      document->GetAllocator());
  pool->AddMember(
      "total_admitted", metrics_.total_admitted->GetValue(), document->GetAllocator());
  pool->AddMember(
      "total_rejected", metrics_.total_rejected->GetValue(), document->GetAllocator());
  pool->AddMember(
      "total_timed_out", metrics_.total_timed_out->GetValue(), document->GetAllocator());
  pool->AddMember("pool_max_mem_resources", metrics_.pool_max_mem_resources->GetValue(),
      document->GetAllocator());
  pool->AddMember("pool_max_requests", metrics_.pool_max_requests->GetValue(),
      document->GetAllocator());
  pool->AddMember(
      "pool_max_queued", metrics_.pool_max_queued->GetValue(), document->GetAllocator());
  pool->AddMember("max_query_mem_limit", metrics_.max_query_mem_limit->GetValue(),
      document->GetAllocator());
  pool->AddMember("min_query_mem_limit", metrics_.min_query_mem_limit->GetValue(),
      document->GetAllocator());
  pool->AddMember("clamp_mem_limit_query_option",
      metrics_.clamp_mem_limit_query_option->GetValue(), document->GetAllocator());
  pool->AddMember("max_running_queries_multiple",
      metrics_.max_running_queries_multiple->GetValue(), document->GetAllocator());
  pool->AddMember("max_queued_queries_multiple",
      metrics_.max_queued_queries_multiple->GetValue(), document->GetAllocator());
  pool->AddMember("max_memory_multiple", metrics_.max_memory_multiple->GetValue(),
      document->GetAllocator());
  pool->AddMember("max_running_queries_derived",
      metrics_.max_running_queries_derived->GetValue(), document->GetAllocator());
  pool->AddMember("max_queued_queries_derived",
      metrics_.max_queued_queries_derived->GetValue(), document->GetAllocator());
  pool->AddMember("max_memory_derived", metrics_.max_memory_derived->GetValue(),
      document->GetAllocator());
  pool->AddMember("wait_time_ms_ema", wait_time_ms_ema_, document->GetAllocator());
  Value histogram(kArrayType);
  for (int bucket = 0; bucket < peak_mem_histogram_.size(); bucket++) {
    Value histogram_elem(kArrayType);
    histogram_elem.PushBack(bucket, document->GetAllocator());
    histogram_elem.PushBack(peak_mem_histogram_[bucket], document->GetAllocator());
    histogram.PushBack(histogram_elem, document->GetAllocator());
  }
  pool->AddMember("peak_mem_usage_histogram", histogram, document->GetAllocator());
}

void AdmissionController::ResetPoolInformationalStats(const string& pool_name) {
  lock_guard<mutex> lock(admission_ctrl_lock_);
  auto it = pool_stats_.find(pool_name);
  if(it == pool_stats_.end()) return;
  it->second.ResetInformationalStats();
}

void AdmissionController::ResetAllPoolInformationalStats() {
  lock_guard<mutex> lock(admission_ctrl_lock_);
  for (auto& it: pool_stats_) it.second.ResetInformationalStats();
}

void AdmissionController::PoolStats::ResetInformationalStats() {
  std::fill(peak_mem_histogram_.begin(), peak_mem_histogram_.end(), 0);
  wait_time_ms_ema_ = 0.0;
  // Reset only metrics keeping track of totals since last reset.
  metrics()->total_admitted->SetValue(0);
  metrics()->total_rejected->SetValue(0);
  metrics()->total_queued->SetValue(0);
  metrics()->total_dequeued->SetValue(0);
  metrics()->total_timed_out->SetValue(0);
  metrics()->total_released->SetValue(0);
  metrics()->time_in_queue_ms->SetValue(0);
}

void AdmissionController::PoolStats::InitMetrics() {
  metrics_.total_admitted = parent_->metrics_group_->AddCounter(
      TOTAL_ADMITTED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.total_queued = parent_->metrics_group_->AddCounter(
      TOTAL_QUEUED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.total_dequeued = parent_->metrics_group_->AddCounter(
      TOTAL_DEQUEUED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.total_rejected = parent_->metrics_group_->AddCounter(
      TOTAL_REJECTED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.total_timed_out = parent_->metrics_group_->AddCounter(
      TOTAL_TIMED_OUT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.total_released = parent_->metrics_group_->AddCounter(
      TOTAL_RELEASED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.time_in_queue_ms = parent_->metrics_group_->AddCounter(
      TIME_IN_QUEUE_METRIC_KEY_FORMAT, 0, name_);

  metrics_.agg_num_running = parent_->metrics_group_->AddGauge(
      AGG_NUM_RUNNING_METRIC_KEY_FORMAT, 0, name_);
  metrics_.agg_num_queued = parent_->metrics_group_->AddGauge(
      AGG_NUM_QUEUED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.agg_mem_reserved = parent_->metrics_group_->AddGauge(
      AGG_MEM_RESERVED_METRIC_KEY_FORMAT, 0, name_);

  metrics_.local_mem_admitted = parent_->metrics_group_->AddGauge(
      LOCAL_MEM_ADMITTED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.local_num_admitted_running = parent_->metrics_group_->AddGauge(
      LOCAL_NUM_ADMITTED_RUNNING_METRIC_KEY_FORMAT, 0, name_);
  metrics_.local_num_queued = parent_->metrics_group_->AddGauge(
      LOCAL_NUM_QUEUED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.local_backend_mem_usage = parent_->metrics_group_->AddGauge(
      LOCAL_BACKEND_MEM_USAGE_METRIC_KEY_FORMAT, 0, name_);
  metrics_.local_backend_mem_reserved = parent_->metrics_group_->AddGauge(
      LOCAL_BACKEND_MEM_RESERVED_METRIC_KEY_FORMAT, 0, name_);

  metrics_.pool_max_mem_resources = parent_->metrics_group_->AddGauge(
      POOL_MAX_MEM_RESOURCES_METRIC_KEY_FORMAT, 0, name_);
  metrics_.pool_max_requests = parent_->metrics_group_->AddGauge(
      POOL_MAX_REQUESTS_METRIC_KEY_FORMAT, 0, name_);
  metrics_.pool_max_queued = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUEUED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_query_mem_limit = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.min_query_mem_limit = parent_->metrics_group_->AddGauge(
      POOL_MIN_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.clamp_mem_limit_query_option = parent_->metrics_group_->AddProperty<bool>(
      POOL_CLAMP_MEM_LIMIT_QUERY_OPTION_METRIC_KEY_FORMAT, false, name_);
  metrics_.max_running_queries_multiple = parent_->metrics_group_->AddDoubleGauge(
      POOL_MAX_RUNNING_QUERIES_MULTIPLE_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_queued_queries_multiple = parent_->metrics_group_->AddDoubleGauge(
      POOL_MAX_QUEUED_QUERIES_MULTIPLE_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_memory_multiple = parent_->metrics_group_->AddGauge(
      POOL_MAX_MEMORY_MULTIPLE_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_running_queries_derived = parent_->metrics_group_->AddGauge(
      POOL_MAX_RUNNING_QUERIES_DERIVED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_queued_queries_derived = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUEUED_QUERIES_DERIVED_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_memory_derived = parent_->metrics_group_->AddGauge(
      POOL_MAX_MEMORY_DERIVED_METRIC_KEY_FORMAT, 0, name_);
}

void AdmissionController::PopulatePerHostMemReservedAndAdmitted(
    PerHostStats* per_host_stats) {
  lock_guard<mutex> l(admission_ctrl_lock_);
  *per_host_stats = host_stats_;
}

string AdmissionController::MakePoolTopicKey(
    const string& pool_name, const string& backend_id) {
  // Ensure the backend_id does not contain the delimiter to ensure that the topic key
  // can be parsed properly by finding the last instance of the delimiter.
  DCHECK_EQ(backend_id.find(TOPIC_KEY_DELIMITER), string::npos);
  return Substitute("$0$1$2", pool_name, TOPIC_KEY_DELIMITER, backend_id);
}

void AdmissionController::GetExecutorGroupsForPool(
    const ClusterMembershipMgr::ExecutorGroups& all_groups, const string& pool_name,
    vector<const ExecutorGroup*>* matching_groups) {
  string prefix(pool_name + POOL_GROUP_DELIMITER);
  // We search for matching groups before the health check so that we don't fall back to
  // the default group in case there are matching but unhealthy groups.
  for (const auto& it : all_groups) {
    StringPiece name(it.first);
    if (name.starts_with(prefix)) matching_groups->push_back(&it.second);
  }
  if (matching_groups->empty()) {
    auto default_it = all_groups.find(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
    if (default_it == all_groups.end()) return;
    VLOG(3) << "Checking default executor group for pool " << pool_name;
    matching_groups->push_back(&default_it->second);
  }
  // Filter out unhealthy groups.
  auto erase_from = std::remove_if(matching_groups->begin(), matching_groups->end(),
      [](const ExecutorGroup* g) { return !g->IsHealthy(); });
  matching_groups->erase(erase_from, matching_groups->end());
  // Sort executor groups by name.
  auto cmp = [](const ExecutorGroup* a, const ExecutorGroup* b) {
    return a->name() < b->name();
  };
  sort(matching_groups->begin(), matching_groups->end(), cmp);
}

int64_t AdmissionController::GetClusterSize(
    const ClusterMembershipMgr::Snapshot& membership_snapshot) {
  int64_t sum = 0;
  for (const auto& it : membership_snapshot.executor_groups) {
    sum += it.second.NumExecutors();
  }
  return sum;
}

int64_t AdmissionController::GetExecutorGroupSize(
    const ClusterMembershipMgr::Snapshot& membership_snapshot,
    const string& group_name) {
  auto it = membership_snapshot.executor_groups.find(group_name);
  DCHECK(it != membership_snapshot.executor_groups.end())
      << "Could not find group " << group_name;
  if (it == membership_snapshot.executor_groups.end()) return 0;
  return it->second.NumExecutors();
}

int64_t AdmissionController::GetMaxMemForPool(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_memory_multiple > 0) {
    return pool_config.max_memory_multiple * cluster_size;
  }
  return pool_config.max_mem_resources;
}

string AdmissionController::GetMaxMemForPoolDescription(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_memory_multiple > 0) {
    return Substitute("calculated as $0 backends each with $1", cluster_size,
        PrintBytes(pool_config.max_memory_multiple));
  }
  return "configured statically";
}

int64_t AdmissionController::GetMaxRequestsForPool(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_running_queries_multiple > 0) {
    return ceil(pool_config.max_running_queries_multiple * cluster_size);
  }
  return pool_config.max_requests;
}

string AdmissionController::GetMaxRequestsForPoolDescription(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_running_queries_multiple > 0) {
    return Substitute("calculated as $0 backends each with $1 queries", cluster_size,
        pool_config.max_running_queries_multiple);
  }
  return "configured statically";
}

int64_t AdmissionController::GetMaxQueuedForPool(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_queued_queries_multiple > 0) {
    return ceil(pool_config.max_queued_queries_multiple * cluster_size);
  }
  return pool_config.max_queued;
}

string AdmissionController::GetMaxQueuedForPoolDescription(
    const TPoolConfig& pool_config, int64_t cluster_size) {
  if (pool_config.max_queued_queries_multiple > 0) {
    return Substitute("calculated as $0 backends each with $1 queries", cluster_size,
        pool_config.max_queued_queries_multiple);
  }
  return "configured statically";
}

bool AdmissionController::PoolDisabled(const TPoolConfig& pool_config) {
  return ((pool_config.max_requests == 0 && pool_config.max_running_queries_multiple == 0)
      || (pool_config.max_mem_resources == 0 && pool_config.max_memory_multiple == 0));
}

bool AdmissionController::PoolLimitsRunningQueriesCount(const TPoolConfig& pool_config) {
  return pool_config.max_requests > 0 || pool_config.max_running_queries_multiple > 0;
}

bool AdmissionController::PoolHasFixedMemoryLimit(const TPoolConfig& pool_config) {
  return pool_config.max_mem_resources > 0 && pool_config.max_memory_multiple <= 0;
}

int64_t AdmissionController::GetMemToAdmit(
    const QuerySchedule& schedule, const BackendExecParams& backend_exec_params) {
  return backend_exec_params.is_coord_backend ? schedule.coord_backend_mem_to_admit() :
                                                schedule.per_backend_mem_to_admit();
}

void AdmissionController::UpdateExecGroupMetricMap(
    ClusterMembershipMgr::SnapshotPtr snapshot) {
  std::unordered_set<string> grp_names;
  for (const auto& group : snapshot->executor_groups) {
    if (group.second.NumHosts() > 0) grp_names.insert(group.first);
  }
  lock_guard<mutex> l(admission_ctrl_lock_);
  auto it = exec_group_query_load_map_.begin();
  while (it != exec_group_query_load_map_.end()) {
    // Erase existing groups from the set so that only new ones are left.
    if (grp_names.erase(it->first) == 0) {
      // Existing group not in the set means it no longer exists.
      string group_name = it->first;
      it = exec_group_query_load_map_.erase(it);
      metrics_group_->RemoveMetric(EXEC_GROUP_QUERY_LOAD_KEY_FORMAT, group_name);
    } else {
      ++it;
    }
  }
  // Now only the new groups are remaining in the set, add a metric for them.
  for (const string& new_grp : grp_names) {
    // There might be lingering queries from when this group was active.
    int64_t currently_running = 0;
    auto new_grp_it = snapshot->executor_groups.find(new_grp);
    DCHECK(new_grp_it != snapshot->executor_groups.end());
    ExecutorGroup group = new_grp_it->second;
    for (const TBackendDescriptor& be_desc : group.GetAllExecutorDescriptors()) {
      const string& host = TNetworkAddressToString(be_desc.address);
      auto stats = host_stats_.find(host);
      if (stats != host_stats_.end()) {
        currently_running = std::max(currently_running, stats->second.num_admitted);
      }
    }
    exec_group_query_load_map_[new_grp] = metrics_group_->AddGauge(
        EXEC_GROUP_QUERY_LOAD_KEY_FORMAT, currently_running, new_grp);
  }
}

void AdmissionController::UpdateExecGroupMetric(
    const string& grp_name, int64_t delta) {
  auto entry = exec_group_query_load_map_.find(grp_name);
  if (entry != exec_group_query_load_map_.end()) entry->second->Increment(delta);
}

} // namespace impala
