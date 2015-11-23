// Copyright 2014 Cloudera Inc.
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

#include "scheduling/admission-controller.h"

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/mem_fn.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "scheduling/simple-scheduler.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "util/runtime-profile.h"

#include "common/names.h"

using namespace strings;

DEFINE_int64(queue_wait_timeout_ms, 60 * 1000, "Maximum amount of time (in "
    "milliseconds) that a request will wait to be admitted before timing out.");

namespace impala {

const string AdmissionController::IMPALA_REQUEST_QUEUE_TOPIC("impala-request-queue");

// Delimiter used for topic keys of the form "<pool_name><delimiter><backend_id>".
// "!" is used because the backend id contains a colon, but it should not contain "!".
// When parsing the topic key we need to be careful to find the last instance in
// case the pool name contains it as well.
const char TOPIC_KEY_DELIMITER = '!';

// Define metric key format strings for metrics in PoolMetrics
// '$0' is replaced with the pool name by strings::Substitute
const string LOCAL_ADMITTED_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-admitted";
const string LOCAL_QUEUED_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-queued";
const string LOCAL_DEQUEUED_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-dequeued";
const string LOCAL_REJECTED_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-rejected";
const string LOCAL_TIMED_OUT_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-timed-out";
const string LOCAL_COMPLETED_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-completed";
const string LOCAL_TIME_IN_QUEUE_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-time-in-queue-ms";
const string CLUSTER_NUM_RUNNING_METRIC_KEY_FORMAT =
  "admission-controller.$0.cluster-num-running";
const string CLUSTER_IN_QUEUE_METRIC_KEY_FORMAT =
  "admission-controller.$0.cluster-in-queue";
const string CLUSTER_MEM_USAGE_METRIC_KEY_FORMAT =
  "admission-controller.$0.cluster-mem-usage";
const string CLUSTER_MEM_ESTIMATE_METRIC_KEY_FORMAT =
  "admission-controller.$0.cluster-mem-estimate";
const string LOCAL_NUM_RUNNING_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-num-running";
const string LOCAL_IN_QUEUE_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-in-queue";
const string LOCAL_MEM_USAGE_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-mem-usage";
const string LOCAL_MEM_ESTIMATE_METRIC_KEY_FORMAT =
  "admission-controller.$0.local-mem-estimate";

// Profile query events
const string QUERY_EVENT_SUBMIT_FOR_ADMISSION = "Submit for admission";
const string QUERY_EVENT_COMPLETED_ADMISSION = "Completed admission";

// Profile info string for admission result
const string PROFILE_INFO_KEY_ADMISSION_RESULT = "Admission result";
const string PROFILE_INFO_VAL_ADMIT_IMMEDIATELY = "Admitted immediately";
const string PROFILE_INFO_VAL_ADMIT_QUEUED = "Admitted (queued)";
const string PROFILE_INFO_VAL_REJECTED = "Rejected";
const string PROFILE_INFO_VAL_TIME_OUT = "Timed out (queued)";

// Error status string formats
// $0 = pool, $1 = rejection reason (see REASON_XXX below)
const string STATUS_REJECTED = "Rejected query from pool $0 : $1";
const string REASON_DISABLED_MEM_LIMIT = "disabled by mem limit set to 0";
const string REASON_DISABLED_REQUESTS_LIMIT = "disabled by requests limit set to 0";
const string REASON_QUEUE_FULL = "queue full, limit=$0, num_queued=$1";
const string REASON_REQ_OVER_MEM_LIMIT =
    "request memory estimate $0 is greater than pool limit $1.\n\n"
    "If the memory estimate appears to be too high, use the MEM_LIMIT query option to "
    "override the memory estimates in admission decisions. Check the explain plan for "
    "warnings about missing stats. Running COMPUTE STATS may help. You may also "
    "consider using query hints to manually improve the plan.\n\n"
    "See the Impala documentation for more details regarding the MEM_LIMIT query "
    "option, table stats, and query hints. If the memory estimate is still too high, "
    "consider modifying the query to reduce the memory impact or increasing the "
    "available memory.";

// Queue decision details
// $0 = num running queries, $1 = num queries limit
const string QUEUED_NUM_RUNNING = "number of running queries $0 is over limit $1";
// $0 = query estimate, $1 = current pool memory estimate, $2 = pool memory limit
const string QUEUED_MEM_LIMIT = "query memory estimate $0 plus current pool "
    "memory estimate $1 is over pool memory limit $2";
// $0 = queue size
const string QUEUED_QUEUE_NOT_EMPTY = "queue is not empty (size $0); queued queries are "
    "executed first";
// $0 = timeout in milliseconds, $1 = queue detail
const string STATUS_TIME_OUT = "Admission for query exceeded timeout $0ms. Queued "
    "reason: $1";

// Parses the pool name and backend_id from the topic key if it is valid.
// Returns true if the topic key is valid and pool_name and backend_id are set.
static inline bool ParsePoolTopicKey(const string& topic_key, string* pool_name,
    string* backend_id) {
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

// Returns a debug string for the given local and total pool stats. Either
// 'total_stats' or 'local_stats' may be NULL to skip writing those stats.
static string DebugPoolStats(const string& pool_name,
    const TPoolStats* total_stats,
    const TPoolStats* local_stats) {
  stringstream ss;
  ss << "pool=" << pool_name;
  if (total_stats != NULL) {
    ss << " Total(";
    ss << "num_running=" << total_stats->num_running << ", ";
    ss << "num_queued=" << total_stats->num_queued << ", ";
    ss << "mem_usage=" <<
        PrettyPrinter::Print(total_stats->mem_usage, TUnit::BYTES) << ", ";
    ss << "mem_estimate=" <<
        PrettyPrinter::Print(total_stats->mem_estimate, TUnit::BYTES);
    ss << ")";
  }
  if (local_stats != NULL) {
    ss << " Local(";
    ss << "num_running=" << local_stats->num_running << ", ";
    ss << "num_queued=" << local_stats->num_queued << ", ";
    ss << "mem_usage=" <<
        PrettyPrinter::Print(local_stats->mem_usage, TUnit::BYTES) << ", ";
    ss << "mem_estimate=" <<
        PrettyPrinter::Print(local_stats->mem_estimate, TUnit::BYTES);
    ss << ")";
  }
  return ss.str();
}

AdmissionController::AdmissionController(RequestPoolService* request_pool_service,
    MetricGroup* metrics, const string& backend_id)
    : request_pool_service_(request_pool_service),
      metrics_(metrics),
      backend_id_(backend_id),
      thrift_serializer_(false),
      done_(false) {
  dequeue_thread_.reset(new Thread("scheduling", "admission-thread",
        &AdmissionController::DequeueLoop, this));
}

AdmissionController::~AdmissionController() {
  // The AdmissionController should live for the lifetime of the impalad, but
  // for unit tests we need to ensure that no thread is waiting on the
  // condition variable. This notifies the dequeue thread to stop and waits
  // for it to finish.
  {
    // Lock to ensure the dequeue thread will see the update to done_
    lock_guard<mutex> l(admission_ctrl_lock_);
    done_ = true;
    dequeue_cv_.notify_one();
  }
  dequeue_thread_->Join();
}

Status AdmissionController::Init(StatestoreSubscriber* subscriber) {
  StatestoreSubscriber::UpdateCallback cb =
    bind<void>(mem_fn(&AdmissionController::UpdatePoolStats), this, _1, _2);
  Status status = subscriber->AddTopic(IMPALA_REQUEST_QUEUE_TOPIC, true, cb);
  if (!status.ok()) {
    status.AddDetail("AdmissionController failed to register request queue topic");
  }
  return status;
}

Status AdmissionController::CanAdmitRequest(const string& pool_name,
    const int64_t max_requests, const int64_t mem_limit, const QuerySchedule& schedule,
    bool admit_from_queue) {
  const TPoolStats& total_stats = cluster_pool_stats_[pool_name];
  DCHECK_GE(total_stats.mem_usage, 0);
  DCHECK_GE(total_stats.mem_estimate, 0);
  const int64_t query_total_estimated_mem = schedule.GetClusterMemoryEstimate();
  const int64_t current_cluster_estimate_mem =
      max(total_stats.mem_usage, total_stats.mem_estimate);
  // The estimated total memory footprint for the query cluster-wise after admitting
  const int64_t cluster_estimated_memory = query_total_estimated_mem +
      current_cluster_estimate_mem;
  DCHECK_GE(cluster_estimated_memory, 0);

  // Can't admit if:
  //  (a) Already over the maximum number of requests
  //  (b) Request will go over the mem limit
  //  (c) This is not admitting from the queue and there are already queued requests
  if (max_requests >= 0 && total_stats.num_running >= max_requests) {
    return Status::Expected(Substitute(QUEUED_NUM_RUNNING, total_stats.num_running,
        max_requests));
  } else if (mem_limit >= 0 && cluster_estimated_memory >= mem_limit) {
    return Status::Expected(Substitute(QUEUED_MEM_LIMIT,
        PrettyPrinter::Print(query_total_estimated_mem, TUnit::BYTES),
        PrettyPrinter::Print(current_cluster_estimate_mem, TUnit::BYTES),
        PrettyPrinter::Print(mem_limit, TUnit::BYTES)));
  } else if (!admit_from_queue && total_stats.num_queued > 0) {
    return Status::Expected(Substitute(QUEUED_QUEUE_NOT_EMPTY, total_stats.num_queued));
  }
  return Status::OK();
}

Status AdmissionController::RejectRequest(const string& pool_name,
    const int64_t max_requests, const int64_t mem_limit, const int64_t max_queued,
    const QuerySchedule& schedule) {
  TPoolStats* total_stats = &cluster_pool_stats_[pool_name];
  const int64_t expected_mem_usage = schedule.GetClusterMemoryEstimate();
  string reject_reason;
  if (max_requests == 0) {
    reject_reason = REASON_DISABLED_REQUESTS_LIMIT;
  } else if (mem_limit == 0) {
    reject_reason = REASON_DISABLED_MEM_LIMIT;
  } else if (mem_limit > 0 && expected_mem_usage >= mem_limit) {
    reject_reason = Substitute(REASON_REQ_OVER_MEM_LIMIT,
        PrettyPrinter::Print(expected_mem_usage, TUnit::BYTES),
        PrettyPrinter::Print(mem_limit, TUnit::BYTES));
  } else if (total_stats->num_queued >= max_queued) {
    reject_reason = Substitute(REASON_QUEUE_FULL, max_queued, total_stats->num_queued);
  } else {
    return Status::OK(); // Not rejected
  }
  return Status(Substitute(STATUS_REJECTED, pool_name, reject_reason));
}

Status AdmissionController::AdmitQuery(QuerySchedule* schedule) {
  const string& pool_name = schedule->request_pool();
  TPoolConfigResult pool_config;
  RETURN_IF_ERROR(request_pool_service_->GetPoolConfig(pool_name, &pool_config));
  const int64_t max_requests = pool_config.max_requests;
  const int64_t max_queued = pool_config.max_queued;
  const int64_t mem_limit = pool_config.mem_limit;

  // Note the queue_node will not exist in the queue when this method returns.
  QueueNode queue_node(*schedule);
  Status admitStatus; // An error status specifies why query is not admitted

  schedule->query_events()->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);
  ScopedEvent completedEvent(schedule->query_events(), QUERY_EVENT_COMPLETED_ADMISSION);
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    RequestQueue* queue = &request_queue_map_[pool_name];
    pool_config_cache_[pool_name] = pool_config;
    PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
    TPoolStats* total_stats = &cluster_pool_stats_[pool_name];
    TPoolStats* local_stats = &local_pool_stats_[pool_name];
    const int64_t cluster_mem_estimate = schedule->GetClusterMemoryEstimate();
    VLOG_QUERY << "Schedule for id=" << schedule->query_id()
               << " in pool_name=" << pool_name << " PoolConfig(max_requests="
               << max_requests << " max_queued=" << max_queued
               << " mem_limit=" << PrettyPrinter::Print(mem_limit, TUnit::BYTES)
               << ") query cluster_mem_estimate="
               << PrettyPrinter::Print(cluster_mem_estimate, TUnit::BYTES);
    VLOG_QUERY << "Stats: " << DebugPoolStats(pool_name, total_stats, local_stats);

    admitStatus = CanAdmitRequest(pool_name, max_requests, mem_limit, *schedule, false);
    if (admitStatus.ok()) {
      // Execute immediately
      pools_for_updates_.insert(pool_name);
      // The local and total stats get incremented together when we queue so if
      // there were any locally queued queries we should not admit immediately.
      DCHECK_EQ(local_stats->num_queued, 0);
      schedule->set_is_admitted(true);
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_ADMIT_IMMEDIATELY);
      ++total_stats->num_running;
      ++local_stats->num_running;
      int64_t mem_estimate = schedule->GetClusterMemoryEstimate();
      local_stats->mem_estimate += mem_estimate;
      total_stats->mem_estimate += mem_estimate;
      if (pool_metrics != NULL) {
        pool_metrics->local_admitted->Increment(1L);
        pool_metrics->local_mem_estimate->Increment(mem_estimate);
        pool_metrics->cluster_mem_estimate->Increment(mem_estimate);
      }
      VLOG_QUERY << "Admitted query id=" << schedule->query_id();
      VLOG_RPC << "Final: " << DebugPoolStats(pool_name, total_stats, local_stats);
      return Status::OK();
    }

    Status rejectStatus = RejectRequest(pool_name, max_requests, mem_limit, max_queued,
        *schedule);
    if (!rejectStatus.ok()) {
      schedule->set_is_admitted(false);
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_REJECTED);
      if (pool_metrics != NULL) pool_metrics->local_rejected->Increment(1L);
      return rejectStatus;
    }

    // We cannot immediately admit but do not need to reject, so queue the request
    VLOG_QUERY << "Queuing, query id=" << schedule->query_id();
    DCHECK_LT(total_stats->num_queued, max_queued);
    DCHECK(max_requests > 0 || mem_limit > 0);
    pools_for_updates_.insert(pool_name);
    ++local_stats->num_queued;
    ++total_stats->num_queued;
    queue->Enqueue(&queue_node);
    if (pool_metrics != NULL) pool_metrics->local_queued->Increment(1L);
  }

  int64_t wait_start_ms = MonotonicMillis();
  int64_t queue_wait_timeout_ms = max(0L, FLAGS_queue_wait_timeout_ms);
  // We just call Get() to block until the result is set or it times out. Note that we
  // don't hold the admission_ctrl_lock_ while we wait on this promise so we need to
  // check the state after acquiring the lock in order to avoid any races because it is
  // Set() by the dequeuing thread while holding admission_ctrl_lock_.
  // TODO: handle cancellation
  bool timed_out;
  queue_node.is_admitted.Get(queue_wait_timeout_ms, &timed_out);
  int64_t wait_time_ms = MonotonicMillis() - wait_start_ms;

  // Take the lock in order to check the result of is_admitted as there could be a race
  // with the timeout. If the Get() timed out, then we need to dequeue the request.
  // Otherwise, the request was admitted and we update the number of running queries
  // stats.
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    RequestQueue* queue = &request_queue_map_[pool_name];
    PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
    pools_for_updates_.insert(pool_name);
    if (pool_metrics != NULL) {
      pool_metrics->local_time_in_queue_ms->Increment(wait_time_ms);
    }
    // Now that we have the lock, check again if the query was actually admitted (i.e.
    // if the promise still hasn't been set), in which case we just admit the query.
    timed_out = !queue_node.is_admitted.IsSet();
    TPoolStats* total_stats = &cluster_pool_stats_[pool_name];
    TPoolStats* local_stats = &local_pool_stats_[pool_name];
    if (timed_out) {
      queue->Remove(&queue_node);
      queue_node.is_admitted.Set(false);
      schedule->set_is_admitted(false);
      schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
          PROFILE_INFO_VAL_TIME_OUT);
      --local_stats->num_queued;
      --total_stats->num_queued;
      if (pool_metrics != NULL) pool_metrics->local_timed_out->Increment(1L);
      return Status(Substitute(STATUS_TIME_OUT, queue_wait_timeout_ms,
            admitStatus.GetDetail()));
    }
    // The dequeue thread updates the stats (to avoid a race condition) so we do
    // not change them here.
    DCHECK(queue_node.is_admitted.Get());
    DCHECK(!queue->Contains(&queue_node));
    schedule->set_is_admitted(true);
    schedule->summary_profile()->AddInfoString(PROFILE_INFO_KEY_ADMISSION_RESULT,
        PROFILE_INFO_VAL_ADMIT_QUEUED);
    if (pool_metrics != NULL) pool_metrics->local_admitted->Increment(1L);
    VLOG_QUERY << "Admitted queued query id=" << schedule->query_id();
    VLOG_RPC << "Final: " << DebugPoolStats(pool_name, total_stats, local_stats);
    return Status::OK();
  }
}

Status AdmissionController::ReleaseQuery(QuerySchedule* schedule) {
  if (!schedule->is_admitted()) return Status::OK(); // No-op if query was not admitted
  const string& pool_name = schedule->request_pool();
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    TPoolStats* total_stats = &cluster_pool_stats_[pool_name];
    TPoolStats* local_stats = &local_pool_stats_[pool_name];
    DCHECK_GT(total_stats->num_running, 0);
    DCHECK_GT(local_stats->num_running, 0);
    --total_stats->num_running;
    --local_stats->num_running;

    int64_t mem_estimate = schedule->GetClusterMemoryEstimate();
    local_stats->mem_estimate -= mem_estimate;
    total_stats->mem_estimate -= mem_estimate;
    PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
    if (pool_metrics != NULL) {
      pool_metrics->local_completed->Increment(1L);
      pool_metrics->local_mem_estimate->Increment(-1 * mem_estimate);
      pool_metrics->cluster_mem_estimate->Increment(-1 * mem_estimate);
    }
    pools_for_updates_.insert(pool_name);
    VLOG_RPC << "Released query id=" << schedule->query_id() << " "
             << DebugPoolStats(pool_name, total_stats, local_stats);
  }
  dequeue_cv_.notify_one();
  return Status::OK();
}

// Statestore subscriber callback for IMPALA_REQUEST_QUEUE_TOPIC. First, add any local
// pool stats updates. Then, per_backend_pool_stats_map_ is updated with the updated
// stats from any topic deltas that are received and we recompute the cluster-wide
// aggregate stats.
void AdmissionController::UpdatePoolStats(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    BOOST_FOREACH(PoolStatsMap::value_type& entry, local_pool_stats_) {
      UpdateLocalMemUsage(entry.first);
    }
    AddPoolUpdates(subscriber_topic_updates);

    StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
        incoming_topic_deltas.find(IMPALA_REQUEST_QUEUE_TOPIC);
    if (topic != incoming_topic_deltas.end()) {
      const TTopicDelta& delta = topic->second;
      // Delta and non-delta updates are handled the same way, except for a full update
      // we first clear the per_backend_pool_stats_map_. We then update the global map
      // and then re-compute the pool stats for any pools that changed.
      if (!delta.is_delta) {
        VLOG_ROW << "Full impala-request-queue stats update";
        per_backend_pool_stats_map_.clear();
      }
      HandleTopicUpdates(delta.topic_entries);
      HandleTopicDeletions(delta.topic_deletions);
    }
    BOOST_FOREACH(PoolStatsMap::value_type& entry, local_pool_stats_) {
      UpdateClusterAggregates(entry.first);
    }
  }
  dequeue_cv_.notify_one(); // Dequeue and admit queries on the dequeue thread
}

void AdmissionController::HandleTopicUpdates(const vector<TTopicItem>& topic_updates) {
  BOOST_FOREACH(const TTopicItem& item, topic_updates) {
    string pool_name;
    string topic_backend_id;
    if (!ParsePoolTopicKey(item.key, &pool_name, &topic_backend_id)) continue;
    // The topic entry from this subscriber is handled specially; the stats coming
    // from the statestore are likely already outdated.
    if (topic_backend_id == backend_id_) continue;
    local_pool_stats_[pool_name]; // Create an entry in the local map if it doesn't exist
    TPoolStats pool_update;
    uint32_t len = item.value.size();
    Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &pool_update);
    if (!status.ok()) {
      VLOG_QUERY << "Error deserializing pool update with key: " << item.key;
      continue;
    }
    PoolStatsMap& pool_map = per_backend_pool_stats_map_[pool_name];

    // Debug logging
    if (pool_map.find(topic_backend_id) != pool_map.end()) {
      VLOG_ROW << "Stats update for key=" << item.key << " previous: "
               << DebugPoolStats(pool_name, NULL, &pool_map[topic_backend_id]);
    }
    VLOG_ROW << "Stats update for key=" << item.key << " updated: "
             << DebugPoolStats(pool_name, NULL, &pool_update);

    pool_map[topic_backend_id] = pool_update;
    DCHECK(per_backend_pool_stats_map_[pool_name][topic_backend_id].num_running ==
        pool_update.num_running);
    DCHECK(per_backend_pool_stats_map_[pool_name][topic_backend_id].num_queued ==
        pool_update.num_queued);
  }
}

void AdmissionController::HandleTopicDeletions(const vector<string>& topic_deletions) {
  BOOST_FOREACH(const string& topic_key, topic_deletions) {
    string pool_name;
    string topic_backend_id;
    if (!ParsePoolTopicKey(topic_key, &pool_name, &topic_backend_id)) continue;
    PoolStatsMap& pool_map = per_backend_pool_stats_map_[pool_name];
    VLOG_ROW << "Deleting stats for key=" << topic_key << " "
             << DebugPoolStats(pool_name, NULL, &pool_map[topic_backend_id]);
    pool_map.erase(topic_backend_id);
    DCHECK(per_backend_pool_stats_map_[pool_name].find(topic_backend_id) ==
           per_backend_pool_stats_map_[pool_name].end());
  }
}

void AdmissionController::UpdateClusterAggregates(const string& pool_name) {
  const TPoolStats& local_stats = local_pool_stats_[pool_name];
  const PoolStatsMap& pool_map = per_backend_pool_stats_map_[pool_name];
  TPoolStats total_stats;
  BOOST_FOREACH(const PoolStatsMap::value_type& entry, pool_map) {
    // Skip an update from this subscriber as the information may be outdated.
    // The current local_stats will be added below.
    if (entry.first == backend_id_) continue;
    DCHECK_GE(entry.second.num_running, 0);
    DCHECK_GE(entry.second.num_queued, 0);
    DCHECK_GE(entry.second.mem_usage, 0);
    DCHECK_GE(entry.second.mem_estimate, 0);
    total_stats.num_running += entry.second.num_running;
    total_stats.num_queued += entry.second.num_queued;
    total_stats.mem_usage += entry.second.mem_usage;
    total_stats.mem_estimate += entry.second.mem_estimate;
  }
  total_stats.num_running += local_stats.num_running;
  total_stats.num_queued += local_stats.num_queued;
  total_stats.mem_usage += local_stats.mem_usage;
  total_stats.mem_estimate += local_stats.mem_estimate;

  DCHECK_GE(total_stats.num_running, 0);
  DCHECK_GE(total_stats.num_queued, 0);
  DCHECK_GE(total_stats.mem_usage, 0);
  DCHECK_GE(total_stats.mem_estimate, 0);
  DCHECK_GE(total_stats.num_running, local_stats.num_running);
  DCHECK_GE(total_stats.num_queued, local_stats.num_queued);

  cluster_pool_stats_[pool_name] = total_stats;
  PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
  if (pool_metrics != NULL) {
    pool_metrics->cluster_num_running->set_value(total_stats.num_running);
    pool_metrics->cluster_in_queue->set_value(total_stats.num_queued);
    pool_metrics->cluster_mem_usage->set_value(total_stats.mem_usage);
    pool_metrics->cluster_mem_estimate->set_value(total_stats.mem_estimate);
  }

  if (cluster_pool_stats_[pool_name] != total_stats) {
    VLOG_ROW << "Recomputed stats, previous: "
             << DebugPoolStats(pool_name, &cluster_pool_stats_[pool_name], NULL);
    VLOG_ROW << "Recomputed stats, updated: "
             << DebugPoolStats(pool_name, &total_stats, NULL);
  }
}

void AdmissionController::UpdateLocalMemUsage(const string& pool_name) {
  TPoolStats* stats = &local_pool_stats_[pool_name];
  MemTracker* tracker = MemTracker::GetRequestPoolMemTracker(pool_name, NULL);
  const int64_t current_usage = tracker == NULL ? 0L : tracker->consumption();
  if (current_usage != stats->mem_usage) {
    stats->mem_usage = current_usage;
    pools_for_updates_.insert(pool_name);
    PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
    if (pool_metrics != NULL) {
      pool_metrics->local_mem_usage->set_value(current_usage);
    }
  }
}

void AdmissionController::AddPoolUpdates(vector<TTopicDelta>* topic_updates) {
  if (pools_for_updates_.empty()) return;
  topic_updates->push_back(TTopicDelta());
  TTopicDelta& topic_delta = topic_updates->back();
  topic_delta.topic_name = IMPALA_REQUEST_QUEUE_TOPIC;
  BOOST_FOREACH(const string& pool_name, pools_for_updates_) {
    DCHECK(local_pool_stats_.find(pool_name) != local_pool_stats_.end());
    TPoolStats& pool_stats = local_pool_stats_[pool_name];
    VLOG_ROW << "Sending topic update " << DebugPoolStats(pool_name, NULL, &pool_stats);
    topic_delta.topic_entries.push_back(TTopicItem());
    TTopicItem& topic_item = topic_delta.topic_entries.back();
    topic_item.key = MakePoolTopicKey(pool_name, backend_id_);
    Status status = thrift_serializer_.Serialize(&pool_stats, &topic_item.value);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to serialize query pool stats: " << status.GetDetail();
      topic_updates->pop_back();
    }
    PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
    if (pool_metrics != NULL) {
      pool_metrics->local_num_running->set_value(pool_stats.num_running);
      pool_metrics->local_in_queue->set_value(pool_stats.num_queued);
      pool_metrics->local_mem_usage->set_value(pool_stats.mem_usage);
    }
  }
  pools_for_updates_.clear();
}

void AdmissionController::DequeueLoop() {
  while (true) {
    unique_lock<mutex> lock(admission_ctrl_lock_);
    if (done_) break;
    dequeue_cv_.wait(lock);
    BOOST_FOREACH(PoolStatsMap::value_type& entry, local_pool_stats_) {
      const string& pool_name = entry.first;
      TPoolStats* local_stats = &entry.second;

      PoolConfigMap::iterator it = pool_config_cache_.find(pool_name);
      if (it == pool_config_cache_.end()) continue; // No local requests in this pool
      const TPoolConfigResult& pool_config = it->second;

      const int64_t max_requests = pool_config.max_requests;
      const int64_t mem_limit = pool_config.mem_limit;

      // We should never have queued any requests in pools where either limit is 0 as no
      // requests should ever be admitted or when both limits are less than 0, i.e.
      // unlimited requests can be admitted and should never be queued.
      if (max_requests == 0 || mem_limit == 0 || (max_requests < 0 && mem_limit < 0)) {
        DCHECK_EQ(local_stats->num_queued, 0);
      }

      if (local_stats->num_queued == 0) continue; // Nothing to dequeue
      DCHECK(max_requests > 0 || mem_limit > 0);
      TPoolStats* total_stats = &cluster_pool_stats_[pool_name];

      DCHECK_GT(local_stats->num_queued, 0);
      DCHECK_GE(total_stats->num_queued, local_stats->num_queued);

      // Determine the maximum number of requests that can possibly be dequeued based
      // on the max_requests limit and the current queue size. We will attempt to
      // dequeue up to this number of requests until reaching the per-pool memory limit.
      int64_t max_to_dequeue = 0;
      if (max_requests > 0) {
        const int64_t total_available = max_requests - total_stats->num_running;
        if (total_available <= 0) continue;
        double queue_size_ratio = static_cast<double>(local_stats->num_queued) /
            static_cast<double>(total_stats->num_queued);
        // The maximum number of requests that can possibly be dequeued is the total
        // number of available requests scaled by the ratio of the size of the local
        // queue to the size of the total queue. We attempt to dequeue at least one
        // request and at most the size of the local queue.
        // TODO: Use a simple heuristic rather than a lower bound of 1 to avoid admitting
        // too many requests globally when only a single request can be admitted.
        max_to_dequeue = min(local_stats->num_queued,
            max(1L, static_cast<int64_t>(queue_size_ratio * total_available)));
      } else {
        max_to_dequeue = local_stats->num_queued; // No limit on num running requests
      }

      RequestQueue& queue = request_queue_map_[pool_name];
      VLOG_RPC << "Dequeue thread will try to admit " << max_to_dequeue << " requests"
               << ", pool=" << pool_name << ", num_queued=" << local_stats->num_queued;

      PoolMetrics* pool_metrics = GetPoolMetrics(pool_name);
      while (max_to_dequeue > 0 && !queue.empty()) {
        QueueNode* queue_node = queue.head();
        DCHECK(queue_node != NULL);
        DCHECK(!queue_node->is_admitted.IsSet());
        const QuerySchedule& schedule = queue_node->schedule;
        Status admitStatus = CanAdmitRequest(pool_name, max_requests, mem_limit,
            schedule, true);
        if (!admitStatus.ok()) {
          VLOG_RPC << "Could not dequeue query id=" << queue_node->schedule.query_id()
                   << " reason: " << admitStatus.GetDetail();
          break;
        }
        queue.Dequeue();
        --local_stats->num_queued;
        --total_stats->num_queued;
        ++local_stats->num_running;
        ++total_stats->num_running;
        int64_t mem_estimate = schedule.GetClusterMemoryEstimate();
        local_stats->mem_estimate += mem_estimate;
        total_stats->mem_estimate += mem_estimate;
        if (pool_metrics != NULL) {
          pool_metrics->local_dequeued->Increment(1L);
          pool_metrics->local_mem_estimate->Increment(mem_estimate);
          pool_metrics->cluster_mem_estimate->Increment(mem_estimate);
        }
        VLOG_ROW << "Dequeuing query id=" << queue_node->schedule.query_id();
        queue_node->is_admitted.Set(true);
        --max_to_dequeue;
      }
      pools_for_updates_.insert(pool_name);
    }
  }
}

AdmissionController::PoolMetrics*
AdmissionController::GetPoolMetrics(const string& pool_name) {
  if (metrics_ == NULL) return NULL;
  PoolMetricsMap::iterator it = pool_metrics_map_.find(pool_name);
  if (it != pool_metrics_map_.end()) return &it->second;

  PoolMetrics* pool_metrics = &pool_metrics_map_[pool_name];
  pool_metrics->local_admitted = metrics_->AddCounter(
      LOCAL_ADMITTED_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_queued = metrics_->AddCounter(
      LOCAL_QUEUED_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_dequeued = metrics_->AddCounter(
      LOCAL_DEQUEUED_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_rejected = metrics_->AddCounter(
      LOCAL_REJECTED_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_timed_out = metrics_->AddCounter(
      LOCAL_TIMED_OUT_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_completed = metrics_->AddCounter(
      LOCAL_COMPLETED_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_time_in_queue_ms = metrics_->AddCounter(
      LOCAL_TIME_IN_QUEUE_METRIC_KEY_FORMAT, 0L, pool_name);

  pool_metrics->cluster_num_running = metrics_->AddGauge(
      CLUSTER_NUM_RUNNING_METRIC_KEY_FORMAT, 0L, pool_name);

  pool_metrics->cluster_in_queue = metrics_->AddGauge(
      CLUSTER_IN_QUEUE_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->cluster_mem_usage = metrics_->AddGauge(
      CLUSTER_MEM_USAGE_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->cluster_mem_estimate = metrics_->AddGauge(
      CLUSTER_MEM_ESTIMATE_METRIC_KEY_FORMAT, 0L, pool_name);

  pool_metrics->local_num_running = metrics_->AddGauge(
      LOCAL_NUM_RUNNING_METRIC_KEY_FORMAT, 0L, pool_name);

  pool_metrics->local_in_queue = metrics_->AddGauge(
      LOCAL_IN_QUEUE_METRIC_KEY_FORMAT, 0L, pool_name);

  pool_metrics->local_mem_usage = metrics_->AddGauge(
      LOCAL_MEM_USAGE_METRIC_KEY_FORMAT, 0L, pool_name);
  pool_metrics->local_mem_estimate = metrics_->AddGauge(
      LOCAL_MEM_ESTIMATE_METRIC_KEY_FORMAT, 0L, pool_name);
  return pool_metrics;
}
}
