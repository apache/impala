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
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/scheduler.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"

#include "common/names.h"

using namespace strings;

DEFINE_int64(queue_wait_timeout_ms, 60 * 1000, "Maximum amount of time (in "
    "milliseconds) that a request will wait to be admitted before timing out.");

namespace impala {

/// Convenience method.
std::string PrintBytes(int64_t value) {
  return PrettyPrinter::Print(value, TUnit::BYTES);
}

int64_t GetProcMemLimit() {
  return ExecEnv::GetInstance()->process_mem_tracker()->limit();
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

// Profile query events
const string QUERY_EVENT_SUBMIT_FOR_ADMISSION = "Submit for admission";
const string QUERY_EVENT_QUEUED = "Queued";
const string QUERY_EVENT_COMPLETED_ADMISSION = "Completed admission";

// Profile info strings
const string PROFILE_INFO_KEY_ADMISSION_RESULT = "Admission result";
const string PROFILE_INFO_VAL_ADMIT_IMMEDIATELY = "Admitted immediately";
const string PROFILE_INFO_VAL_QUEUED = "Queued";
const string PROFILE_INFO_VAL_ADMIT_QUEUED = "Admitted (queued)";
const string PROFILE_INFO_VAL_REJECTED = "Rejected";
const string PROFILE_INFO_VAL_TIME_OUT = "Timed out (queued)";
const string PROFILE_INFO_KEY_QUEUE_DETAIL = "Admission queue details";
const string PROFILE_INFO_VAL_QUEUE_DETAIL = "waited $0 ms, reason: $1";

// Error status string details
const string REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation is greater than memory available to the query "
    "for buffer reservations. Memory reservation needed given the current plan: $0. Set "
    "mem_limit to at least $1. Note that changing the mem_limit may also change the "
    "plan. See the query profile for more information about the per-node memory "
    "requirements.";
const string REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation is greater than memory available to the query "
    "for buffer reservations. Increase the buffer_pool_limit to $0. See the query "
    "profile for more information about the per-node memory requirements.";
const string REASON_MIN_RESERVATION_OVER_POOL_MEM =
    "minimum memory reservation needed is greater than pool max mem resources. Pool "
    "max mem resources: $0. Cluster-wide memory reservation needed: $1. Increase the "
    "pool max mem resources. See the query profile for more information about the "
    "per-node memory requirements.";
const string REASON_DISABLED_MAX_MEM_RESOURCES =
    "disabled by pool max mem resources set to 0";
const string REASON_DISABLED_REQUESTS_LIMIT = "disabled by requests limit set to 0";
const string REASON_QUEUE_FULL = "queue full, limit=$0, num_queued=$1";
const string REASON_REQ_OVER_POOL_MEM =
    "request memory needed $0 is greater than pool max mem resources $1.\n\n"
    "Use the MEM_LIMIT query option to indicate how much memory is required per node. "
    "The total memory needed is the per-node MEM_LIMIT times the number of nodes "
    "executing the query. See the Admission Control documentation for more information.";
const string REASON_REQ_OVER_NODE_MEM =
    "request memory needed $0 per node is greater than process mem limit $1.\n\n"
    "Use the MEM_LIMIT query option to indicate how much memory is required per node.";

// Queue decision details
// $0 = num running queries, $1 = num queries limit
const string QUEUED_NUM_RUNNING = "number of running queries $0 is over limit $1";
// $0 = queue size
const string QUEUED_QUEUE_NOT_EMPTY = "queue is not empty (size $0); queued queries are "
    "executed first";
// $0 = pool name, $1 = pool max memory, $2 = pool mem needed, $3 = pool mem available
const string POOL_MEM_NOT_AVAILABLE = "Not enough aggregate memory available in pool $0 "
    "with max mem resources $1. Needed $2 but only $3 was available.";
// $0 = host name, $1 = host mem needed, $3 = host mem available
const string HOST_MEM_NOT_AVAILABLE = "Not enough memory available on host $0."
    "Needed $1 but only $2 was available.";

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

// Returns the topic key for the pool at this backend, i.e. a string of the
// form: "<pool_name><delimiter><backend_id>".
static inline string MakePoolTopicKey(const string& pool_name,
    const string& backend_id) {
  // Ensure the backend_id does not contain the delimiter to ensure that the topic key
  // can be parsed properly by finding the last instance of the delimiter.
  DCHECK_EQ(backend_id.find(TOPIC_KEY_DELIMITER), string::npos);
  return Substitute("$0$1$2", pool_name, TOPIC_KEY_DELIMITER, backend_id);
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
      metrics_group_(metrics),
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
  StatestoreSubscriber::UpdateCallback cb =
    bind<void>(mem_fn(&AdmissionController::UpdatePoolStats), this, _1, _2);
  Status status = subscriber_->AddTopic(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, true, cb);
  if (!status.ok()) {
    status.AddDetail("AdmissionController failed to register request queue topic");
  }
  return status;
}

void AdmissionController::PoolStats::Admit(const QuerySchedule& schedule) {
  int64_t mem_admitted = schedule.GetClusterMemoryEstimate();
  local_mem_admitted_ += mem_admitted;
  metrics_.local_mem_admitted->Increment(mem_admitted);

  agg_num_running_ += 1;
  metrics_.agg_num_running->Increment(1L);

  local_stats_.num_admitted_running += 1;
  metrics_.local_num_admitted_running->Increment(1L);

  metrics_.total_admitted->Increment(1L);
}

void AdmissionController::PoolStats::Release(const QuerySchedule& schedule) {
  int64_t mem_admitted = schedule.GetClusterMemoryEstimate();
  local_mem_admitted_ -= mem_admitted;
  metrics_.local_mem_admitted->Increment(-mem_admitted);

  agg_num_running_ -= 1;
  metrics_.agg_num_running->Increment(-1L);

  local_stats_.num_admitted_running -= 1;
  metrics_.local_num_admitted_running->Increment(-1L);

  metrics_.total_released->Increment(1L);
  DCHECK_GE(local_stats_.num_admitted_running, 0);
  DCHECK_GE(agg_num_running_, 0);
  DCHECK_GE(local_mem_admitted_, 0);
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
  int64_t per_node_mem_needed = schedule.GetPerHostMemoryEstimate();
  int64_t cluster_mem_needed = schedule.GetClusterMemoryEstimate();

  // Case 1:
  PoolStats* stats = GetPoolStats(pool_name);
  VLOG_RPC << "Checking agg mem in pool=" << pool_name << " : " << stats->DebugString()
           << " cluster_mem_needed=" << PrintBytes(cluster_mem_needed)
           << " pool_max_mem=" << PrintBytes(pool_max_mem);
  if (stats->EffectiveMemReserved() + cluster_mem_needed > pool_max_mem) {
    *mem_unavailable_reason = Substitute(POOL_MEM_NOT_AVAILABLE, pool_name,
        PrintBytes(pool_max_mem), PrintBytes(cluster_mem_needed),
        PrintBytes(max(pool_max_mem - stats->EffectiveMemReserved(), 0L)));
    return false;
  }

  // Case 2:
  int64_t proc_mem_limit = GetProcMemLimit();
  for (const auto& entry : schedule.per_backend_exec_params()) {
    const TNetworkAddress& host = entry.first;
    const string host_id = TNetworkAddressToString(host);
    int64_t mem_reserved = host_mem_reserved_[host_id];
    int64_t mem_admitted = host_mem_admitted_[host_id];
    VLOG_ROW << "Checking memory on host=" << host_id
             << " mem_reserved=" << PrintBytes(mem_reserved)
             << " mem_admitted=" << PrintBytes(mem_admitted)
             << " needs=" << PrintBytes(per_node_mem_needed)
             << " proc_limit=" << PrintBytes(proc_mem_limit);
    int64_t effective_host_mem_reserved = std::max(mem_reserved, mem_admitted);
    if (effective_host_mem_reserved + per_node_mem_needed > proc_mem_limit) {
      *mem_unavailable_reason = Substitute(HOST_MEM_NOT_AVAILABLE, host_id,
          PrintBytes(per_node_mem_needed),
          PrintBytes(max(proc_mem_limit - effective_host_mem_reserved, 0L)));
      return false;
    }
  }

  return true;
}

bool AdmissionController::CanAdmitRequest(const QuerySchedule& schedule,
    const TPoolConfig& pool_cfg, bool admit_from_queue, string* not_admitted_reason) {
  const string& pool_name = schedule.request_pool();
  PoolStats* stats = GetPoolStats(pool_name);

  // Can't admit if:
  //  (a) There are already queued requests (and this is not admitting from the queue).
  //  (b) Already at the maximum number of requests
  //  (c) Request will go over the mem limit
  if (!admit_from_queue && stats->local_stats().num_queued > 0) {
    *not_admitted_reason = Substitute(QUEUED_QUEUE_NOT_EMPTY,
        stats->local_stats().num_queued);
    return false;
  } else if (pool_cfg.max_requests >= 0 &&
      stats->agg_num_running() >= pool_cfg.max_requests) {
    *not_admitted_reason = Substitute(QUEUED_NUM_RUNNING, stats->agg_num_running(),
        pool_cfg.max_requests);
    return false;
  } else if (!HasAvailableMemResources(schedule, pool_cfg, not_admitted_reason)) {
    return false;
  }
  return true;
}

bool AdmissionController::RejectImmediately(QuerySchedule* schedule,
    const TPoolConfig& pool_cfg, string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());
  // This function checks for a number of cases where the query can be rejected
  // immediately. The first check that fails is the error that is reported. The order of
  // the checks isn't particularly important, though some thought was given to ordering
  // them in a way that might make the sense for a user.

  // Compute the max (over all backends) min_reservation_bytes and the cluster total
  // (across all backends) min_reservation_bytes.
  int64_t max_min_reservation_bytes = -1;
  int64_t cluster_min_reservation_bytes = 0;
  for (const auto& e: schedule->per_backend_exec_params()) {
    cluster_min_reservation_bytes += e.second.min_reservation_bytes;
    if (e.second.min_reservation_bytes > max_min_reservation_bytes) {
      max_min_reservation_bytes = e.second.min_reservation_bytes;
    }
  }

  // Checks related to the min buffer reservation against configured query memory limits:
  if (schedule->query_options().__isset.buffer_pool_limit &&
      schedule->query_options().buffer_pool_limit > 0) {
    if (max_min_reservation_bytes > schedule->query_options().buffer_pool_limit) {
      *rejection_reason = Substitute(REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION,
          PrintBytes(max_min_reservation_bytes));
      return true;
    }
  } else if (schedule->query_options().__isset.mem_limit &&
      schedule->query_options().mem_limit > 0) {
    const int64_t mem_limit = schedule->query_options().mem_limit;
    const int64_t max_reservation =
        ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
    if (max_min_reservation_bytes > max_reservation) {
      const int64_t required_mem_limit =
          ReservationUtil::GetMinMemLimitFromReservation(max_min_reservation_bytes);
      *rejection_reason = Substitute(REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION,
          PrintBytes(max_min_reservation_bytes), PrintBytes(required_mem_limit));
      return true;
    }
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
  if (pool_cfg.max_mem_resources > 0 &&
      cluster_min_reservation_bytes > pool_cfg.max_mem_resources) {
    *rejection_reason = Substitute(REASON_MIN_RESERVATION_OVER_POOL_MEM,
        PrintBytes(pool_cfg.max_mem_resources),
        PrintBytes(cluster_min_reservation_bytes));
    return true;
  }
  if (pool_cfg.max_mem_resources > 0 &&
      schedule->GetClusterMemoryEstimate() > pool_cfg.max_mem_resources) {
    *rejection_reason = Substitute(REASON_REQ_OVER_POOL_MEM,
        PrintBytes(schedule->GetClusterMemoryEstimate()),
        PrintBytes(pool_cfg.max_mem_resources));
    return true;
  }
  if (pool_cfg.max_mem_resources > 0 &&
      schedule->GetPerHostMemoryEstimate() > GetProcMemLimit()) {
    *rejection_reason = Substitute(REASON_REQ_OVER_NODE_MEM,
        PrintBytes(schedule->GetPerHostMemoryEstimate()), PrintBytes(GetProcMemLimit()));
    return true;
  }

  // Checks related to the pool queue size:
  PoolStats* stats = GetPoolStats(schedule->request_pool());
  if (stats->agg_num_queued() >= pool_cfg.max_queued) {
    *rejection_reason = Substitute(REASON_QUEUE_FULL, pool_cfg.max_queued,
        stats->agg_num_queued());
    return true;
  }

  return false;
}

void AdmissionController::PoolStats::UpdateConfigMetrics(const TPoolConfig& pool_cfg) {
  metrics_.pool_max_mem_resources->SetValue(pool_cfg.max_mem_resources);
  metrics_.pool_max_requests->SetValue(pool_cfg.max_requests);
  metrics_.pool_max_queued->SetValue(pool_cfg.max_queued);
}

Status AdmissionController::AdmitQuery(QuerySchedule* schedule) {
  const string& pool_name = schedule->request_pool();
  TPoolConfig pool_cfg;
  RETURN_IF_ERROR(request_pool_service_->GetPoolConfig(pool_name, &pool_cfg));
  const int64_t max_requests = pool_cfg.max_requests;
  const int64_t max_queued = pool_cfg.max_queued;
  const int64_t max_mem = pool_cfg.max_mem_resources;

  // Note the queue_node will not exist in the queue when this method returns.
  QueueNode queue_node(*schedule);
  string not_admitted_reason;

  schedule->query_events()->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);
  ScopedEvent completedEvent(schedule->query_events(), QUERY_EVENT_COMPLETED_ADMISSION);
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pool_config_map_[pool_name] = pool_cfg;
    PoolStats* stats = GetPoolStats(pool_name);
    stats->UpdateConfigMetrics(pool_cfg);
    VLOG_QUERY << "Schedule for id=" << schedule->query_id() << " in pool_name="
               << pool_name << " cluster_mem_needed="
               << PrintBytes(schedule->GetClusterMemoryEstimate())
               << " PoolConfig: max_requests=" << max_requests << " max_queued="
               << max_queued << " max_mem=" << PrintBytes(max_mem);
    VLOG_QUERY << "Stats: " << stats->DebugString();
    string rejection_reason;
    if (RejectImmediately(schedule, pool_cfg, &rejection_reason)) {
      schedule->set_is_admitted(false);
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
      VLOG_QUERY << "Admitted query id=" << schedule->query_id();
      stats->Admit(*schedule);
      UpdateHostMemAdmitted(*schedule, schedule->GetPerHostMemoryEstimate());
      schedule->set_is_admitted(true);
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_ADMIT_IMMEDIATELY);
      VLOG_RPC << "Final: " << stats->DebugString();
      return Status::OK();
    }

    // We cannot immediately admit but do not need to reject, so queue the request
    VLOG_QUERY << "Queuing, query id=" << schedule->query_id();
    stats->Queue(*schedule);
    queue->Enqueue(&queue_node);
  }

  // Update the profile info before waiting. These properties will be updated with
  // their final state after being dequeued.
  schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
      PROFILE_INFO_VAL_QUEUED);
  schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_QUEUE_DETAIL,
      not_admitted_reason);
  schedule->query_events()->MarkEvent(QUERY_EVENT_QUEUED);

  int64_t queue_wait_timeout_ms = FLAGS_queue_wait_timeout_ms;
  if (pool_cfg.__isset.queue_timeout_ms) {
    queue_wait_timeout_ms = pool_cfg.queue_timeout_ms;
  }
  queue_wait_timeout_ms = max<int64_t>(0, queue_wait_timeout_ms);
  int64_t wait_start_ms = MonotonicMillis();

  // We just call Get() to block until the result is set or it times out. Note that we
  // don't hold the admission_ctrl_lock_ while we wait on this promise so we need to
  // check the state after acquiring the lock in order to avoid any races because it is
  // Set() by the dequeuing thread while holding admission_ctrl_lock_.
  // TODO: handle cancellation
  bool timed_out;
  queue_node.is_admitted.Get(queue_wait_timeout_ms, &timed_out);
  int64_t wait_time_ms = MonotonicMillis() - wait_start_ms;
  schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_QUEUE_DETAIL,
      Substitute(PROFILE_INFO_VAL_QUEUE_DETAIL, wait_time_ms, not_admitted_reason));

  // Take the lock in order to check the result of is_admitted as there could be a race
  // with the timeout. If the Get() timed out, then we need to dequeue the request.
  // Otherwise, the request was admitted and we update the number of running queries
  // stats.
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pools_for_updates_.insert(pool_name);
    PoolStats* stats = GetPoolStats(pool_name);
    stats->metrics()->time_in_queue_ms->Increment(wait_time_ms);
    // Now that we have the lock, check again if the query was actually admitted (i.e.
    // if the promise still hasn't been set), in which case we just admit the query.
    timed_out = !queue_node.is_admitted.IsSet();
    if (timed_out) {
      queue->Remove(&queue_node);
      queue_node.is_admitted.Set(false);
      schedule->set_is_admitted(false);
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_TIME_OUT);
      stats->Dequeue(*schedule, true);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_TIMED_OUT,
          queue_wait_timeout_ms, pool_name, not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    }
    // The dequeue thread updates the stats (to avoid a race condition) so we do
    // not change them here.
    DCHECK(queue_node.is_admitted.Get());
    DCHECK(!queue->Contains(&queue_node));
    schedule->set_is_admitted(true);
    schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
        PROFILE_INFO_VAL_ADMIT_QUEUED);
    VLOG_QUERY << "Admitted queued query id=" << schedule->query_id();
    VLOG_RPC << "Final: " << stats->DebugString();
    return Status::OK();
  }
}

void AdmissionController::ReleaseQuery(const QuerySchedule& schedule) {
  if (!schedule.is_admitted()) return; // No-op if query was not admitted
  const string& pool_name = schedule.request_pool();
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    PoolStats* stats = GetPoolStats(pool_name);
    stats->Release(schedule);
    UpdateHostMemAdmitted(schedule, -schedule.GetPerHostMemoryEstimate());
    pools_for_updates_.insert(pool_name);
    VLOG_RPC << "Released query id=" << schedule.query_id() << " "
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
    Status status = thrift_serializer_.Serialize(&stats->local_stats(),
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
        if (total_available <= 0) continue;
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

      RequestQueue& queue = request_queue_map_[pool_name];
      VLOG_RPC << "Dequeue thread will try to admit " << max_to_dequeue << " requests"
               << ", pool=" << pool_name << ", num_queued="
               << stats->local_stats().num_queued;

      while (max_to_dequeue > 0 && !queue.empty()) {
        QueueNode* queue_node = queue.head();
        DCHECK(queue_node != nullptr);
        DCHECK(!queue_node->is_admitted.IsSet());
        const QuerySchedule& schedule = queue_node->schedule;
        string not_admitted_reason;
        // TODO: Requests further in the queue may be blocked unnecessarily. Consider a
        // better policy once we have better test scenarios.
        if (!CanAdmitRequest(schedule, pool_config, true, &not_admitted_reason)) {
          VLOG_RPC << "Could not dequeue query id=" << schedule.query_id()
                   << " reason: " << not_admitted_reason;
          break;
        }
        VLOG_RPC << "Dequeuing query=" << schedule.query_id();
        queue.Dequeue();
        stats->Dequeue(schedule, false);
        stats->Admit(schedule);
        UpdateHostMemAdmitted(schedule, schedule.GetPerHostMemoryEstimate());
        queue_node->is_admitted.Set(true);
        --max_to_dequeue;
      }
      pools_for_updates_.insert(pool_name);
    }
  }
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
}
}
