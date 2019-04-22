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
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/scheduler.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
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
const string AdmissionController::PROFILE_INFO_KEY_STALENESS_WARNING =
    "Admission control state staleness";
const string AdmissionController::PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME =
    "AdmissionControlTimeSinceLastUpdate";

// Error status string details
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
    "max mem resources: $0. Cluster-wide memory reservation needed: $1. Increase the "
    "pool max mem resources. See the query profile for more information about the "
    "per-node memory requirements.";
const string REASON_DISABLED_MAX_MEM_RESOURCES =
    "disabled by pool max mem resources set to 0";
const string REASON_DISABLED_REQUESTS_LIMIT = "disabled by requests limit set to 0";
// $2 is the staleness detail.
const string REASON_QUEUE_FULL = "queue full, limit=$0, num_queued=$1.$2";
const string REASON_REQ_OVER_POOL_MEM =
    "request memory needed $0 is greater than pool max mem resources $1.\n\n"
    "Use the MEM_LIMIT query option to indicate how much memory is required per node. "
    "The total memory needed is the per-node MEM_LIMIT times the number of nodes "
    "executing the query. See the Admission Control documentation for more information.";
const string REASON_REQ_OVER_NODE_MEM =
    "request memory needed $0 per node is greater than memory available for admission $1 "
    "of $2.\n\nUse the MEM_LIMIT query option to indicate how much memory is required "
    "per node.";
const string REASON_THREAD_RESERVATION_LIMIT_EXCEEDED =
    "thread reservation on backend '$0' is greater than the THREAD_RESERVATION_LIMIT "
    "query option value: $1 > $2.";
const string REASON_THREAD_RESERVATION_AGG_LIMIT_EXCEEDED =
    "sum of thread reservations across all $0 backends is greater than the "
    "THREAD_RESERVATION_AGGREGATE_LIMIT query option value: $1 > $2.";

// Queue decision details
// $0 = num running queries, $1 = num queries limit, $2 = staleness detail
const string QUEUED_NUM_RUNNING =
    "number of running queries $0 is at or over limit $1.$2";
// $0 = queue size, $1 = staleness detail
const string QUEUED_QUEUE_NOT_EMPTY = "queue is not empty (size $0); queued queries are "
    "executed first.$1";
// $0 = pool name, $1 = pool max memory, $2 = pool mem needed, $3 = pool mem available,
// $4 = staleness detail
const string POOL_MEM_NOT_AVAILABLE = "Not enough aggregate memory available in pool $0 "
    "with max mem resources $1. Needed $2 but only $3 was available.$4";
// $0 = host name, $1 = host mem needed, $3 = host mem available, $4 = staleness detail
const string HOST_MEM_NOT_AVAILABLE = "Not enough memory available on host $0."
    "Needed $1 but only $2 out of $3 was available.$4";

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
AdmissionController::AdmissionController(StatestoreSubscriber* subscriber,
    RequestPoolService* request_pool_service, MetricGroup* metrics,
    const TNetworkAddress& host_addr)
    : subscriber_(subscriber),
      request_pool_service_(request_pool_service),
      metrics_group_(metrics->GetOrCreateChildGroup("admission-controller")),
      host_id_(TNetworkAddressToString(host_addr)),
      thrift_serializer_(false),
      done_(false) {}

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

void AdmissionController::PoolStats::Admit(const QuerySchedule& schedule) {
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

void AdmissionController::PoolStats::Release(
    const QuerySchedule& schedule, int64_t peak_mem_consumption) {
  int64_t cluster_mem_admitted = schedule.GetClusterMemoryToAdmit();
  DCHECK_GT(cluster_mem_admitted, 0);
  local_mem_admitted_ -= cluster_mem_admitted;
  metrics_.local_mem_admitted->Increment(-cluster_mem_admitted);

  agg_num_running_ -= 1;
  metrics_.agg_num_running->Increment(-1L);

  local_stats_.num_admitted_running -= 1;
  metrics_.local_num_admitted_running->Increment(-1L);

  metrics_.total_released->Increment(1L);
  DCHECK_GE(local_stats_.num_admitted_running, 0);
  DCHECK_GE(agg_num_running_, 0);
  DCHECK_GE(local_mem_admitted_, 0);
  int64_t histogram_bucket =
      BitUtil::RoundUp(peak_mem_consumption, HISTOGRAM_BIN_SIZE) / HISTOGRAM_BIN_SIZE;
  histogram_bucket = std::max(std::min(histogram_bucket, HISTOGRAM_NUM_OF_BINS), 1L) - 1;
  peak_mem_histogram_[histogram_bucket] = ++(peak_mem_histogram_[histogram_bucket]);
}

void AdmissionController::PoolStats::Queue(const QuerySchedule& schedule) {
  agg_num_queued_ += 1;
  metrics_.agg_num_queued->Increment(1L);

  local_stats_.num_queued += 1;
  metrics_.local_num_queued->Increment(1L);

  metrics_.total_queued->Increment(1L);
}

void AdmissionController::PoolStats::Dequeue(const QuerySchedule& schedule,
    bool timed_out) {
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

void AdmissionController::UpdateHostMemAdmitted(const QuerySchedule& schedule,
    int64_t per_node_mem) {
  DCHECK_NE(per_node_mem, 0);
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host_addr = entry.first;
    const string host = TNetworkAddressToString(host_addr);
    VLOG_ROW << "Update admitted mem reserved for host=" << host
             << " prev=" << PrintBytes(host_mem_admitted_[host])
             << " new="  << PrintBytes(host_mem_admitted_[host] + per_node_mem);
    host_mem_admitted_[host] += per_node_mem;
    DCHECK_GE(host_mem_admitted_[host], 0);
  }
}

bool AdmissionController::CanAccommodateMaxInitialReservation(
    const QuerySchedule& schedule, const TPoolConfig& pool_cfg,
    string* mem_unavailable_reason) {
  const int64_t per_backend_mem_limit = schedule.per_backend_mem_limit();
  if (per_backend_mem_limit > 0) {
    const int64_t max_reservation =
        ReservationUtil::GetReservationLimitFromMemLimit(per_backend_mem_limit);
    const int64_t largest_min_mem_reservation = schedule.largest_min_reservation();
    if (largest_min_mem_reservation > max_reservation) {
      const int64_t required_mem_limit =
          ReservationUtil::GetMinMemLimitFromReservation(largest_min_mem_reservation);
      *mem_unavailable_reason = Substitute(REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION,
          PrintBytes(largest_min_mem_reservation), PrintBytes(required_mem_limit));
      return false;
    }
  }
  return true;
}

bool AdmissionController::HasAvailableMemResources(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, string* mem_unavailable_reason) {
  const string& pool_name = schedule.request_pool();
  const int64_t pool_max_mem = pool_cfg.max_mem_resources;
  // If the pool doesn't have memory resources configured, always true.
  if (pool_max_mem < 0) return true;

  // Otherwise, two conditions must be met:
  // 1) The memory estimated to be reserved by all queries in this pool *plus* the total
  //    memory needed for this query must be within the max pool memory resources
  //    specified.
  // 2) Each individual backend must have enough mem available within its process limit
  //    to execute the query.
  int64_t per_host_mem_to_admit = schedule.per_backend_mem_to_admit();
  int64_t cluster_mem_to_admit = schedule.GetClusterMemoryToAdmit();

  // Case 1:
  PoolStats* stats = GetPoolStats(pool_name);
  VLOG_RPC << "Checking agg mem in pool=" << pool_name << " : " << stats->DebugString()
           << " cluster_mem_needed=" << PrintBytes(cluster_mem_to_admit)
           << " pool_max_mem=" << PrintBytes(pool_max_mem);
  if (stats->EffectiveMemReserved() + cluster_mem_to_admit > pool_max_mem) {
    *mem_unavailable_reason = Substitute(POOL_MEM_NOT_AVAILABLE, pool_name,
        PrintBytes(pool_max_mem), PrintBytes(cluster_mem_to_admit),
        PrintBytes(max(pool_max_mem - stats->EffectiveMemReserved(), 0L)),
        GetStalenessDetailLocked(" "));
    return false;
  }

  // Case 2:
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host = entry.first;
    const string host_id = TNetworkAddressToString(host);
    int64_t admit_mem_limit = entry.second.admit_mem_limit;
    int64_t mem_reserved = host_mem_reserved_[host_id];
    int64_t mem_admitted = host_mem_admitted_[host_id];
    VLOG_ROW << "Checking memory on host=" << host_id
             << " mem_reserved=" << PrintBytes(mem_reserved)
             << " mem_admitted=" << PrintBytes(mem_admitted)
             << " needs=" << PrintBytes(per_host_mem_to_admit)
             << " admit_mem_limit=" << PrintBytes(admit_mem_limit);
    int64_t effective_host_mem_reserved = std::max(mem_reserved, mem_admitted);
    if (effective_host_mem_reserved + per_host_mem_to_admit > admit_mem_limit) {
      *mem_unavailable_reason =
          Substitute(HOST_MEM_NOT_AVAILABLE, host_id, PrintBytes(per_host_mem_to_admit),
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

bool AdmissionController::CanAdmitRequest(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, bool admit_from_queue, string* not_admitted_reason) {
  // Can't admit if:
  //  (a) Pool configuration is invalid
  //  (b) There are already queued requests (and this is not admitting from the queue).
  //  (c) Already at the maximum number of requests
  //  (d) There are not enough memory resources available for the query

  // Queries from a misconfigured pool will remain queued till they either time out or the
  // pool config is changed to a valid config.
  if (!IsPoolConfigValid(pool_cfg, not_admitted_reason)) return false;

  const string& pool_name = schedule.request_pool();
  PoolStats* stats = GetPoolStats(pool_name);
  if (!admit_from_queue && stats->local_stats().num_queued > 0) {
    *not_admitted_reason = Substitute(QUEUED_QUEUE_NOT_EMPTY,
        stats->local_stats().num_queued, GetStalenessDetailLocked(" "));
    return false;
  } else if (pool_cfg.max_requests >= 0 &&
      stats->agg_num_running() >= pool_cfg.max_requests) {
    *not_admitted_reason = Substitute(QUEUED_NUM_RUNNING, stats->agg_num_running(),
        pool_cfg.max_requests, GetStalenessDetailLocked(" "));
    return false;
  } else if (!HasAvailableMemResources(schedule, pool_cfg, not_admitted_reason)) {
    return false;
  }
  return true;
}

bool AdmissionController::RejectImmediately(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());
  // This function checks for a number of cases where the query can be rejected
  // immediately. The first check that fails is the error that is reported. The order of
  // the checks isn't particularly important, though some thought was given to ordering
  // them in a way that might make the sense for a user.
  if (!IsPoolConfigValid(pool_cfg, rejection_reason)) return true;

  // Compute the max (over all backends) and cluster total (across all backends) for
  // min_mem_reservation_bytes and thread_reservation and the min (over all backends)
  // min_admit_mem_limit.
  pair<const TNetworkAddress*, int64_t> largest_min_mem_reservation(nullptr, -1);
  int64_t cluster_min_mem_reservation_bytes = 0;
  pair<const TNetworkAddress*, int64_t> max_thread_reservation(nullptr, 0);
  pair<const TNetworkAddress*, int64_t> min_admit_mem_limit(
      nullptr, std::numeric_limits<int64_t>::max());
  int64_t cluster_thread_reservation = 0;
  for (const auto& e : schedule.per_backend_exec_params()) {
    cluster_min_mem_reservation_bytes += e.second.min_mem_reservation_bytes;
    if (e.second.min_mem_reservation_bytes > largest_min_mem_reservation.second) {
      largest_min_mem_reservation =
          make_pair(&e.first, e.second.min_mem_reservation_bytes);
    }
    cluster_thread_reservation += e.second.thread_reservation;
    if (e.second.thread_reservation > max_thread_reservation.second) {
      max_thread_reservation = make_pair(&e.first, e.second.thread_reservation);
    }
    if (e.second.admit_mem_limit < min_admit_mem_limit.second) {
      min_admit_mem_limit.first = &e.first;
      min_admit_mem_limit.second = e.second.admit_mem_limit;
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

  // Checks related to pool max_requests:
  if (pool_cfg.max_requests == 0) {
    *rejection_reason = REASON_DISABLED_REQUESTS_LIMIT;
    return true;
  }

  // Checks related to pool max_mem_resources:
  if (pool_cfg.max_mem_resources == 0) {
    *rejection_reason = REASON_DISABLED_MAX_MEM_RESOURCES;
    return true;
  }
  if (pool_cfg.max_mem_resources > 0) {
    if (cluster_min_mem_reservation_bytes > pool_cfg.max_mem_resources) {
      *rejection_reason = Substitute(REASON_MIN_RESERVATION_OVER_POOL_MEM,
          PrintBytes(pool_cfg.max_mem_resources),
          PrintBytes(cluster_min_mem_reservation_bytes));
      return true;
    }
    int64_t cluster_mem_to_admit = schedule.GetClusterMemoryToAdmit();
    if (cluster_mem_to_admit > pool_cfg.max_mem_resources) {
      *rejection_reason = Substitute(REASON_REQ_OVER_POOL_MEM,
          PrintBytes(cluster_mem_to_admit), PrintBytes(pool_cfg.max_mem_resources));
      return true;
    }
    int64_t per_backend_mem_to_admit = schedule.per_backend_mem_to_admit();
    if (per_backend_mem_to_admit > min_admit_mem_limit.second) {
      *rejection_reason = Substitute(REASON_REQ_OVER_NODE_MEM,
          PrintBytes(per_backend_mem_to_admit), PrintBytes(min_admit_mem_limit.second),
          TNetworkAddressToString(*min_admit_mem_limit.first));
      return true;
    }
  }

  // Checks related to the pool queue size:
  PoolStats* stats = GetPoolStats(schedule.request_pool());
  if (stats->agg_num_queued() >= pool_cfg.max_queued) {
    *rejection_reason = Substitute(REASON_QUEUE_FULL, pool_cfg.max_queued,
        stats->agg_num_queued(), GetStalenessDetailLocked(" "));
    return true;
  }

  return false;
}

void AdmissionController::PoolStats::UpdateConfigMetrics(const TPoolConfig& pool_cfg) {
  metrics_.pool_max_mem_resources->SetValue(pool_cfg.max_mem_resources);
  metrics_.pool_max_requests->SetValue(pool_cfg.max_requests);
  metrics_.pool_max_queued->SetValue(pool_cfg.max_queued);
  metrics_.max_query_mem_limit->SetValue(pool_cfg.max_query_mem_limit);
  metrics_.min_query_mem_limit->SetValue(pool_cfg.min_query_mem_limit);
  metrics_.clamp_mem_limit_query_option->SetValue(
      pool_cfg.clamp_mem_limit_query_option);
}

Status AdmissionController::SubmitForAdmission(QuerySchedule* schedule,
    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admit_outcome) {
  const string& pool_name = schedule->request_pool();
  TPoolConfig pool_cfg;
  RETURN_IF_ERROR(request_pool_service_->GetPoolConfig(pool_name, &pool_cfg));
  schedule->UpdateMemoryRequirements(pool_cfg);
  const int64_t max_requests = pool_cfg.max_requests;
  const int64_t max_queued = pool_cfg.max_queued;
  const int64_t max_mem = pool_cfg.max_mem_resources;

  // Note the queue_node will not exist in the queue when this method returns.
  QueueNode queue_node(schedule, admit_outcome, schedule->summary_profile());
  string not_admitted_reason;

  schedule->query_events()->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);
  ScopedEvent completedEvent(schedule->query_events(), QUERY_EVENT_COMPLETED_ADMISSION);
  {
    // Take lock to ensure the Dequeue thread does not modify the request queue.
    lock_guard<mutex> lock(admission_ctrl_lock_);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pool_config_map_[pool_name] = pool_cfg;
    PoolStats* stats = GetPoolStats(pool_name);
    stats->UpdateConfigMetrics(pool_cfg);
    VLOG_QUERY << "Schedule for id=" << PrintId(schedule->query_id()) << " in pool_name="
               << pool_name << " per_host_mem_estimate="
               << PrintBytes(schedule->GetPerHostMemoryEstimate())
               << " PoolConfig: max_requests=" << max_requests << " max_queued="
               << max_queued << " max_mem=" << PrintBytes(max_mem);
    VLOG_QUERY << "Stats: " << stats->DebugString();
    string rejection_reason;
    if (RejectImmediately(*schedule, pool_cfg, &rejection_reason)) {
      AdmissionOutcome outcome =
          admit_outcome->Set(AdmissionOutcome::REJECTED_OR_TIMED_OUT);
      if (outcome != AdmissionOutcome::REJECTED_OR_TIMED_OUT) {
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
        VLOG_QUERY << "Ready to be " << PROFILE_INFO_VAL_REJECTED
                   << " but already cancelled, query id="
                   << PrintId(schedule->query_id());
        return Status::CANCELLED;
      }
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_REJECTED);
      stats->metrics()->total_rejected->Increment(1);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_REJECTED,
          pool_name, rejection_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    }
    pools_for_updates_.insert(pool_name);

    if (CanAdmitRequest(*schedule, pool_cfg, false, &not_admitted_reason)) {
      DCHECK_EQ(stats->local_stats().num_queued, 0);
      AdmissionOutcome outcome = admit_outcome->Set(AdmissionOutcome::ADMITTED);
      if (outcome != AdmissionOutcome::ADMITTED) {
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
        VLOG_QUERY << "Ready to be " << PROFILE_INFO_VAL_ADMIT_IMMEDIATELY
                   << " but already cancelled, query id="
                   << PrintId(schedule->query_id());
        return Status::CANCELLED;
      }
      VLOG_QUERY << "Admitted query id=" << PrintId(schedule->query_id());
      AdmitQuery(schedule, false);
      stats->UpdateWaitTime(0);
      VLOG_RPC << "Final: " << stats->DebugString();
      return Status::OK();
    }

    // We cannot immediately admit but do not need to reject, so queue the request
    VLOG_QUERY << "Queuing, query id=" << PrintId(schedule->query_id())
               << " reason: " << not_admitted_reason;
    stats->Queue(*schedule);
    queue->Enqueue(&queue_node);
  }

  // Update the profile info before waiting. These properties will be updated with
  // their final state after being dequeued.
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_QUEUED);
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_INITIAL_QUEUE_REASON, not_admitted_reason);
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_LAST_QUEUED_REASON, not_admitted_reason);
  schedule->query_events()->MarkEvent(QUERY_EVENT_QUEUED);

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
  schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_INITIAL_QUEUE_REASON,
      Substitute(
          PROFILE_INFO_VAL_INITIAL_QUEUE_REASON, wait_time_ms, not_admitted_reason));

  // Disallow the FAIL action here. It would leave the queue in an inconsistent state.
  DebugActionNoFail(schedule->query_options(), "AC_AFTER_ADMISSION_OUTCOME");

  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    // If the query has not been admitted or cancelled up till now, it will be considered
    // to be timed out.
    AdmissionOutcome outcome =
        admit_outcome->Set(AdmissionOutcome::REJECTED_OR_TIMED_OUT);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pools_for_updates_.insert(pool_name);
    PoolStats* stats = GetPoolStats(pool_name);
    stats->UpdateWaitTime(wait_time_ms);
    if (outcome == AdmissionOutcome::REJECTED_OR_TIMED_OUT) {
      queue->Remove(&queue_node);
      stats->Dequeue(*schedule, true);
      schedule->summary_profile()->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_TIME_OUT);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_TIMED_OUT,
          queue_wait_timeout_ms, pool_name, not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    } else if (outcome == AdmissionOutcome::CANCELLED) {
      // Only update stats if it has not already been removed and updated by the Dequeue
      // thread.
      if (queue->Remove(&queue_node)) stats->Dequeue(*schedule, false);
      schedule->summary_profile()->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_CANCELLED_IN_QUEUE);
      VLOG_QUERY << PROFILE_INFO_VAL_CANCELLED_IN_QUEUE
                 << ", query id=" << PrintId(schedule->query_id());
      return Status::CANCELLED;
    }
    // The dequeue thread updates the stats (to avoid a race condition) so we do
    // not change them here.
    DCHECK_ENUM_EQ(outcome, AdmissionOutcome::ADMITTED);
    DCHECK(!queue->Contains(&queue_node));
    VLOG_QUERY << "Admitted queued query id=" << PrintId(schedule->query_id());
    VLOG_RPC << "Final: " << stats->DebugString();
    return Status::OK();
  }
}

void AdmissionController::ReleaseQuery(
    const QuerySchedule& schedule, int64_t peak_mem_consumption) {
  const string& pool_name = schedule.request_pool();
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    PoolStats* stats = GetPoolStats(pool_name);
    stats->Release(schedule, peak_mem_consumption);
    UpdateHostMemAdmitted(schedule, -schedule.per_backend_mem_to_admit());
    pools_for_updates_.insert(pool_name);
    VLOG_RPC << "Released query id=" << PrintId(schedule.query_id()) << " "
             << stats->DebugString();
  }
  dequeue_cv_.NotifyOne();
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
        for (PoolStatsMap::value_type& entry: pool_stats_) {
          entry.second.ClearRemoteStats();
        }
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
      VLOG_RPC << "Attempted to remove non-existent remote stats for host=" << host_id;
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
  for (const PoolStats::RemoteStatsMap::value_type& remote_entry:
       remote_stats_) {
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

    // The update the per-pool and per-host aggregates with the mem reserved by this
    // host in this pool.
    mem_reserved += remote_pool_stats.backend_mem_reserved;
    (*host_mem_reserved)[host] += remote_pool_stats.backend_mem_reserved;
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

  if (agg_num_running_ == num_running && agg_num_queued_ == num_queued &&
      agg_mem_reserved_ == mem_reserved) {
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
  // Recompute the host mem reserved.
  HostMemMap updated_mem_reserved;
  for (PoolStatsMap::value_type& entry: pool_stats_) {
    entry.second.UpdateAggregates(&updated_mem_reserved);
  }

  if (VLOG_ROW_IS_ON) {
    stringstream ss;
    ss << "Updated mem reserved for hosts:";
    int i = 0;
    for (const HostMemMap::value_type& e: updated_mem_reserved) {
      if (host_mem_reserved_[e.first] == e.second) continue;
      ss << endl << e.first << ": " << PrintBytes(host_mem_reserved_[e.first]);
      ss << " -> " << PrintBytes(e.second);
      ++i;
    }
    if (i > 0) VLOG_ROW << ss.str();
  }
  host_mem_reserved_ = updated_mem_reserved;
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
  for (PoolStatsMap::value_type& entry: pool_stats_) {
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
    for (const PoolConfigMap::value_type& entry: pool_config_map_) {
      const string& pool_name = entry.first;
      const TPoolConfig& pool_config = entry.second;
      const int64_t max_requests = pool_config.max_requests;
      const int64_t max_mem = pool_config.max_mem_resources;
      PoolStatsMap::iterator it = pool_stats_.find(pool_name);
      DCHECK(it != pool_stats_.end());
      PoolStats* stats = &it->second;
      RequestQueue& queue = request_queue_map_[pool_name];

      if (stats->local_stats().num_queued == 0) continue; // Nothing to dequeue

      // Handle the unlikely case that after requests were queued, the pool config was
      // changed and the pool was disabled. Skip dequeuing them and let them time out.
      // TODO: Dequeue and reject all requests in this pool.
      if (max_requests == 0 || max_mem == 0) continue;

      DCHECK_GT(stats->local_stats().num_queued, 0);
      DCHECK_GE(stats->agg_num_queued(), stats->local_stats().num_queued);

      // Use a heuristic to limit the number of requests we dequeue locally to avoid all
      // impalads dequeuing too many requests at the same time. This is based on the
      // max_requests limit and the current queue size. We will attempt to dequeue up to
      // this number of requests until reaching the per-pool memory limit.
      int64_t max_to_dequeue = 0;
      if (max_requests > 0) {
        const int64_t total_available = max_requests - stats->agg_num_running();
        if (total_available <= 0) {
          if (!queue.empty()) {
            LogDequeueFailed(queue.head(),
                Substitute(QUEUED_NUM_RUNNING, stats->agg_num_running(), max_requests,
                    GetStalenessDetailLocked(" ")));
          }
          continue;
        }
        // Use the ratio of locally queued requests to agg queued so that each impalad
        // can dequeue a proportional amount total_available. Note, this offers no
        // fairness between impalads.
        double queue_size_ratio =
            static_cast<double>(stats->local_stats().num_queued) /
            static_cast<double>(stats->agg_num_queued());
        // TODO: Floating point arithmetic may result in dequeuing one less request than
        // it should if the local num_queued is equal to the agg_num_queued.
        max_to_dequeue = min(stats->local_stats().num_queued,
            max<int64_t>(1, queue_size_ratio * total_available));
      } else {
        max_to_dequeue = stats->agg_num_queued(); // No limit on num running requests
      }
      VLOG_RPC << "Dequeue thread will try to admit " << max_to_dequeue << " requests"
               << ", pool=" << pool_name << ", num_queued="
               << stats->local_stats().num_queued;

      while (max_to_dequeue > 0 && !queue.empty()) {
        QueueNode* queue_node = queue.head();
        DCHECK(queue_node != nullptr);
        QuerySchedule* schedule = queue_node->schedule;
        schedule->UpdateMemoryRequirements(pool_config);
        bool is_cancelled = queue_node->admit_outcome->IsSet()
            && queue_node->admit_outcome->Get() == AdmissionOutcome::CANCELLED;
        string not_admitted_reason;
        // TODO: Requests further in the queue may be blocked unnecessarily. Consider a
        // better policy once we have better test scenarios.
        if (!is_cancelled
            && !CanAdmitRequest(*schedule, pool_config, true, &not_admitted_reason)) {
          LogDequeueFailed(queue_node, not_admitted_reason);
          break;
        }
        VLOG_RPC << "Dequeuing query=" << PrintId(schedule->query_id());
        queue.Dequeue();
        --max_to_dequeue;
        stats->Dequeue(*schedule, false);
        // If query is already cancelled, just dequeue and continue.
        AdmissionOutcome outcome =
            queue_node->admit_outcome->Set(AdmissionOutcome::ADMITTED);
        if (outcome == AdmissionOutcome::CANCELLED) {
          VLOG_QUERY << "Dequeued cancelled query=" << PrintId(schedule->query_id());
          continue;
        }
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::ADMITTED);
        AdmitQuery(schedule, true);
      }
      pools_for_updates_.insert(pool_name);
    }
  }
}

void AdmissionController::LogDequeueFailed(QueueNode* node,
    const string& not_admitted_reason) {
  VLOG_QUERY << "Could not dequeue query id="
             << PrintId(node->schedule->query_id())
             << " reason: " << not_admitted_reason;
  node->profile->AddInfoString(PROFILE_INFO_KEY_LAST_QUEUED_REASON,
      not_admitted_reason);
}

AdmissionController::PoolStats*
AdmissionController::GetPoolStats(const string& pool_name) {
  PoolStatsMap::iterator it = pool_stats_.find(pool_name);
  if (it == pool_stats_.end()) {
    pool_stats_.insert(PoolStatsMap::value_type(pool_name, PoolStats(this, pool_name)));
    it = pool_stats_.find(pool_name);
  }
  DCHECK(it != pool_stats_.end());
  return &it->second;
}

bool AdmissionController::IsPoolConfigValid(const TPoolConfig& pool_cfg, string* reason) {
  if (pool_cfg.max_query_mem_limit > 0
      && pool_cfg.min_query_mem_limit > pool_cfg.max_query_mem_limit) {
    *reason = Substitute("Invalid pool config: the min_query_mem_limit is greater than "
                         "the max_query_mem_limit ($0 > $1)",
        pool_cfg.min_query_mem_limit, pool_cfg.max_query_mem_limit);
    return false;
  }
  if (pool_cfg.max_mem_resources > 0
      && pool_cfg.min_query_mem_limit > pool_cfg.max_mem_resources) {
    *reason = Substitute("Invalid pool config: the min_query_mem_limit is greater than "
                         "the max_mem_resources ($0 > $1)",
        pool_cfg.min_query_mem_limit, pool_cfg.max_mem_resources);
    return false;
  }
  return true;
}

void AdmissionController::AdmitQuery(QuerySchedule* schedule, bool was_queued) {
  PoolStats* pool_stats = GetPoolStats(schedule->request_pool());
  VLOG_RPC << "For Query " << schedule->query_id() << " per_backend_mem_limit set to: "
           << PrintBytes(schedule->per_backend_mem_limit())
           << " per_backend_mem_to_admit set to: "
           << PrintBytes(schedule->per_backend_mem_to_admit());
  // Update memory accounting.
  pool_stats->Admit(*schedule);
  UpdateHostMemAdmitted(*schedule, schedule->per_backend_mem_to_admit());
  // Update summary profile.
  const string& admission_result =
      was_queued ? PROFILE_INFO_VAL_ADMIT_QUEUED : PROFILE_INFO_VAL_ADMIT_IMMEDIATELY;
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, admission_result);
  schedule->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMITTED_MEM, PrintBytes(schedule->GetClusterMemoryToAdmit()));
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
  PoolStatsMap::iterator it = pool_stats_.find(pool_name);
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
    QuerySchedule* schedule = node->schedule;
    Value query_info(kObjectType);
    Value query_id(PrintId(schedule->query_id()).c_str(), document->GetAllocator());
    query_info.AddMember("query_id", query_id, document->GetAllocator());
    query_info.AddMember(
        "mem_limit", schedule->per_backend_mem_limit(), document->GetAllocator());
    query_info.AddMember("mem_limit_to_admit", schedule->per_backend_mem_to_admit(),
        document->GetAllocator());
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
  PoolStatsMap::iterator it = pool_stats_.find(pool_name);
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
}

void AdmissionController::PopulatePerHostMemReservedAndAdmitted(
    std::unordered_map<string, pair<int64_t, int64_t>>* mem_map) {
  lock_guard<mutex> l(admission_ctrl_lock_);
  for (const auto& elem: host_mem_reserved_) {
    (*mem_map)[elem.first] = make_pair(elem.second, host_mem_admitted_[elem.first]);
  }
}

string AdmissionController::MakePoolTopicKey(
    const string& pool_name, const string& backend_id) {
  // Ensure the backend_id does not contain the delimiter to ensure that the topic key
  // can be parsed properly by finding the last instance of the delimiter.
  DCHECK_EQ(backend_id.find(TOPIC_KEY_DELIMITER), string::npos);
  return Substitute("$0$1$2", pool_name, TOPIC_KEY_DELIMITER, backend_id);
}
}
