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
#include "scheduling/schedule-state.h"
#include "scheduling/scheduler.h"
#include "service/impala-server.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "common/names.h"

using std::make_pair;
using std::pair;
using namespace strings;

DEFINE_bool(balance_queries_across_executor_groups, false,
    "If true, balance queries across multiple groups that belonging to the same request "
    "pool based on available memory and slots in each executor group. If false, "
    "admission is attempted to groups in alphanumerically sorted order.");

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

DEFINE_bool(clamp_query_mem_limit_backend_mem_limit, true, "Caps query memory limit to "
    "memory limit for admission on the backends. The coordinator memory limit is capped "
    "to the coordinator backend's memory limit, while executor memory limit is capped to "
    "the effective or minimum memory limit for admission on executor backends. If the "
    "flag is not set, a query requesting more than backend's memory limit for admission "
    "gets rejected during admission. However, if this flag is set, such a query gets "
    "admitted with backend's memory limit and could succeed if the memory request was "
    "over estimated and could fail if query really needs more memory." );

DECLARE_bool(is_coordinator);
DECLARE_bool(is_executor);

namespace impala {

const int64_t AdmissionController::PoolStats::HISTOGRAM_NUM_OF_BINS = 128;
const int64_t AdmissionController::PoolStats::HISTOGRAM_BIN_SIZE = 1024L * 1024L * 1024L;
const double AdmissionController::PoolStats::EMA_MULTIPLIER = 0.2;
const int AdmissionController::PoolStats::MAX_NUM_TRIVIAL_QUERY_RUNNING = 3;

/// Convenience method.
string PrintBytes(int64_t value) {
  return PrettyPrinter::Print(value, TUnit::BYTES);
}

// Delimiter used for pool topic keys of the form
// "<prefix><pool_name><delimiter><backend_id>". "!" is used because the backend id
// contains a colon, but it should not contain "!". When parsing the topic key we need to
// be careful to find the last instance in case the pool name contains it as well.
const char TOPIC_KEY_DELIMITER = '!';

// Prefix used by topic keys for pool stat updates.
const string TOPIC_KEY_POOL_PREFIX = "POOL:";
// Prefix used by topic keys for PerHostStat updates.
const string TOPIC_KEY_STAT_PREFIX = "STAT:";

// Delimiter used for the resource pool prefix of executor groups. In order to be used for
// queries in "resource-pool-A", an executor group name must start with
// "resource-pool-A-".
const char POOL_GROUP_DELIMITER = '-';

const string EXEC_GROUP_QUERY_LOAD_KEY_FORMAT =
  "admission-controller.executor-group.num-queries-executing.$0";

const string TOTAL_DEQUEUE_FAILED_COORDINATOR_LIMITED =
  "admission-controller.total-dequeue-failed-coordinator-limited";

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
const string POOL_QUEUE_TIMEOUT_METRIC_KEY_FORMAT =
  "admission-controller.pool-queue-timeout.$0";
const string POOL_MAX_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-query-mem-limit.$0";
const string POOL_MIN_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-min-query-mem-limit.$0";
const string POOL_CLAMP_MEM_LIMIT_QUERY_OPTION_METRIC_KEY_FORMAT =
  "admission-controller.pool-clamp-mem-limit-query-option.$0";
const string POOL_MAX_QUERY_CPU_CORE_PER_NODE_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-query-cpu-core-per-node-limit.$0";
const string POOL_MAX_QUERY_CPU_CORE_COORDINATOR_LIMIT_METRIC_KEY_FORMAT =
  "admission-controller.pool-max-query-cpu-core-coordinator-limit.$0";

// Profile info strings
const string AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT = "Admission result";
const string AdmissionController::PROFILE_INFO_VAL_ADMIT_IMMEDIATELY =
    "Admitted immediately";
const string AdmissionController::PROFILE_INFO_VAL_ADMIT_TRIVIAL =
    "Admitted as a trivial query";
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
const string AdmissionController::PROFILE_INFO_KEY_EXECUTOR_GROUP_QUERY_LOAD =
    "Number of running queries in designated executor group when admitted";
const string AdmissionController::PROFILE_INFO_KEY_STALENESS_WARNING =
    "Admission control state staleness";
const string AdmissionController::PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME =
    "AdmissionControlTimeSinceLastUpdate";

// Error status string details
const string REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM =
    "Invalid pool config: the min_query_mem_limit $0 is greater than the "
    "max_mem_resources $1";
const string REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_LIMIT =
    "Invalid pool config: the min_query_mem_limit is greater than the "
    "max_query_mem_limit ($0 > $1)";
const string REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation is greater than memory available to the query for buffer "
    "reservations. Memory reservation needed given the current plan: $0. Adjust the $1 "
    "for the query to allow the query memory limit to be at least $2. Note that changing "
    "the memory limit may also change the plan. See '$3' in the "
    "query profile for more information about the per-node memory requirements.";
const string REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION =
    "minimum memory reservation on backend '$0' is greater than memory available to the "
    "query for buffer reservations. Increase the buffer_pool_limit to $1. "
    "See '$2' in the query profile for more information about "
    "the per-node memory requirements.";
const string REASON_NOT_ENOUGH_SLOTS_ON_BACKEND =
    "number of admission control slots needed ($0) on backend '$1' is greater than total "
    "slots available $2. Reduce MT_DOP or MAX_FRAGMENT_INSTANCES_PER_NODE to less than "
    "$2 to ensure that the query can execute.";
const string REASON_MIN_RESERVATION_OVER_POOL_MEM =
    "minimum memory reservation needed is greater than pool max mem resources. Pool "
    "max mem resources: $0. Cluster-wide memory reservation needed: $1. Increase the "
    "pool max mem resources. See '$2' in the query profile "
    "for more information about the per-node memory requirements.";
const string REASON_DISABLED_MAX_MEM_RESOURCES =
    "disabled by pool max mem resources set to 0";
const string REASON_DISABLED_REQUESTS_LIMIT = "disabled by requests limit set to 0";
// $2 is the description of how the queue limit was calculated, $3 is the staleness
// detail.
const string REASON_QUEUE_FULL = "queue full, limit=$0, num_queued=$1.$2";
const string REASON_REQ_OVER_POOL_MEM =
    "request memory needed $0 is greater than pool max mem resources $1.\n\n"
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
const string REASON_COORDINATOR_NOT_FOUND =
    "Coordinator not registered with the statestore.";
const string REASON_NO_EXECUTOR_GROUPS =
    "Waiting for executors to start. Only DDL queries and queries scheduled only on the "
    "coordinator (either NUM_NODES set to 1 or when small query optimization is "
    "triggered) can currently run.";

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
const string HOST_MEM_NOT_AVAILABLE = "Not enough memory available on host $0. "
    "Needed $1 but only $2 out of $3 was available.$4";

// $0 = host name, $1 = num admitted, $2 = max requests
const string HOST_SLOT_NOT_AVAILABLE = "Not enough admission control slots available on "
                                       "host $0. Needed $1 slots but $2/$3 are already "
                                       "in use.";

// Parses the topic key to separate the prefix that helps recognize the kind of update
// received.
static inline bool ParseTopicKey(
    const string& topic_key, string* prefix, string* suffix) {
  // The prefix should always be present and the network address must be at least 3
  // characters (including ':' and if the hostname and port are each only 1 character).
  // Then the topic_key must be at least 8 characters (5 + 3).
  const int MIN_TOPIC_KEY_SIZE = 8;
  if (topic_key.length() < MIN_TOPIC_KEY_SIZE) {
    VLOG_QUERY << "Invalid topic key for pool: " << topic_key;
    return false;
  }
  DCHECK_EQ(TOPIC_KEY_POOL_PREFIX.size(), TOPIC_KEY_STAT_PREFIX.size())
      << "All admission topic key prefixes should be of the same size";
  *prefix = topic_key.substr(0, TOPIC_KEY_POOL_PREFIX.size());
  *suffix = topic_key.substr(TOPIC_KEY_POOL_PREFIX.size());
  return true;
}

// Parses the pool name and backend_id from the topic key if it is valid.
// Returns true if the topic key is valid and pool_name and backend_id are set.
static inline bool ParsePoolTopicKey(const string& topic_key, string* pool_name,
    string* backend_id) {
  // Pool topic keys will look something like: poolname!hostname:22000
  size_t pos = topic_key.find_last_of(TOPIC_KEY_DELIMITER);
  if (pos == string::npos || pos >= topic_key.size() - 1) {
    VLOG_QUERY << "Invalid topic key for pool: " << topic_key;
    return false;
  }
  *pool_name = topic_key.substr(0, pos);
  *backend_id = topic_key.substr(pos + 1);
  return true;
}

// Append to ss a debug string for memory consumption part of the pool stats.
// Here is one example.
// topN_query_stats: queries=[554b016cf0f3a37f:9a1bfcfd00000000,
// 464dcd9cc47d724b:9e6a3f6400000000, 2844275a1458bf1f:0bc5887500000000,
// a449dbc7bcbd2af1:647e6ded00000000, 8c430ea5ad38e94a:3c27bf4400000000],
// total_mem_consumed=1.26 MB, fraction_of_pool_total_mem=0.61; pool_level_stats:
// num_running=10, min=0, max=257.48 KB, pool_total_mem=2.06 MB, average_per_query=210.74
// KB
void AdmissionController::PoolStats::AppendStatsForConsumedMemory(
    stringstream& ss, const TPoolStats& stats) {
  ss << "topN_query_stats: ";
  ss << "queries=[";
  int num_ids = stats.heavy_memory_queries.size();
  int64_t total_memory_consumed_by_top_queries = 0;
  for (int i = 0; i < num_ids; i++) {
    auto& query = stats.heavy_memory_queries[i];
    total_memory_consumed_by_top_queries += query.memory_consumed;
    ss << PrintId(query.queryId);
    if (i < num_ids - 1) ss << ", ";
  }
  ss << "], ";
  ss << "total_mem_consumed="
     << PrettyPrinter::PrintBytes(total_memory_consumed_by_top_queries);
  int64_t total_memory_consumed = stats.total_memory_consumed;
  if (total_memory_consumed > 0) {
    ss << ", fraction_of_pool_total_mem=" << setprecision(2)
       << float(total_memory_consumed_by_top_queries) / total_memory_consumed;
  }
  ss << "; ";

  ss << "pool_level_stats: ";
  ss << "num_running=" << stats.num_running << ", ";
  ss << "min=" << PrettyPrinter::PrintBytes(stats.min_memory_consumed) << ", ";
  ss << "max=" << PrettyPrinter::PrintBytes(stats.max_memory_consumed) << ", ";
  ss << "pool_total_mem=" << PrettyPrinter::PrintBytes(total_memory_consumed);
  if (stats.num_running > 0) {
    ss << ", average_per_query="
       << PrettyPrinter::PrintBytes(total_memory_consumed / stats.num_running);
  }
}

// Return a debug string for the pool stats.
string AdmissionController::PoolStats::DebugPoolStats(const TPoolStats& stats) const {
  stringstream ss;
  ss << "num_admitted_running=" << stats.num_admitted_running << ", ";
  ss << "num_queued=" << stats.num_queued << ", ";
  ss << "backend_mem_reserved=" << PrintBytes(stats.backend_mem_reserved) << ", ";
  AppendStatsForConsumedMemory(ss, stats);
  return ss.str();
}

string AdmissionController::PoolStats::DebugString() const {
  stringstream ss;
  ss << "agg_num_running=" << agg_num_running_ << ", ";
  ss << "agg_num_queued=" << agg_num_queued_ << ", ";
  ss << "agg_mem_reserved=" << PrintBytes(agg_mem_reserved_) << ", ";
  ss << " local_host(local_mem_admitted=" << PrintBytes(local_mem_admitted_) << ", ";
  ss << "local_trivial_running=" << local_trivial_running_ << ", ";
  ss << DebugPoolStats(local_stats_) << ")";
  return ss.str();
}

// Output the string 'value with indentation of 'n' space characters.
// When eof is true, append a newline.
static void OutputIndentedString(
    stringstream& ss, int n, const std::string& value, bool eof = true) {
  ss << std::string(n, ' ') << value;
  if (eof) ss << std::endl;
}

// Return a string reporting top 5 queries with most memory consumed among all
// pools in a host.
//
// Here is an example of the output string for two pools.
//    pool_name=root.queueB:
//      topN_query_stats:
//         queries=[
//            id=0000000000000001:0000000000000004, consumed=20.00 MB,
//            id=0000000000000001:0000000000000003, consumed=19.00 MB,
//            id=0000000000000001:0000000000000002, consumed=8.00 MB
//         ],
//         total_consumed=47.00 MB
//         fraction_of_pool_total_mem=0.47
//      all_query_stats:
//         num_running=4,
//         min=5.00 MB,
//         max=20.00 MB,
//         pool_total_mem=100.00 MB,
//         average=25.00 MB
//
//   pool_name=root.queueC:
//      topN_query_stats:
//         queries=[
//            id=0000000000000002:0000000000000000, consumed=18.00 MB,
//            id=0000000000000002:0000000000000001, consumed=12.00 MB
//         ],
//         total_consumed=30.00 MB
//         fraction_of_pool_total_mem=0.06
//      all_query_stats:
//         num_running=40,
//         min=10.00 MB,
//         max=200.00 MB,
//         pool_total_mem=500.00 MB,
//         average=12.50 MB
string AdmissionController::GetLogStringForTopNQueriesOnHost(
    const std::string& host_id) {
  // All heavy memory queries about 'host_id' are the starting point. Collect them
  // into listOfTopNs.
  stringstream ss;
  std::vector<Item> listOfTopNs;
  for (auto& it : pool_stats_) {
    const TPoolStats* tpool_stats = (host_id_ == host_id) ?
        &(it.second.local_stats()) :
        it.second.FindTPoolStatsForRemoteHost(host_id);
    if (!tpool_stats) continue;
    for (auto& query : tpool_stats->heavy_memory_queries) {
      listOfTopNs.emplace_back(
          Item(query.memory_consumed, it.first, query.queryId, tpool_stats));
    }
  }
  // If the number of items is 0, no need to go any further.
  if (listOfTopNs.size() == 0) return "";

  // Sort the list in descending order of memory consumed, pool name, qid and
  // the address of TPoolStats.
  sort(listOfTopNs.begin(), listOfTopNs.end(), std::greater<Item>());

  // Decide the number of topN items to report from the list
  int items = (listOfTopNs.size() >= 5) ? 5 : listOfTopNs.size();
  // Keep first 'items' items and remove the rest.
  listOfTopNs.resize(items);

  int indent = 0;
  OutputIndentedString(ss, indent, "", true);
  OutputIndentedString(ss, indent, std::string("Stats for host ") + host_id);

  // Use an integer vector to remember the indices of items in listOfTopNs
  // that belong to the same pool.
  std::vector<int> indices;
  while (items > 0) {
    // The first item in the list becomes 'current'.
    indices.clear();
    auto& current = listOfTopNs[0];
    indices.push_back(0);
    // Look for all other items with identical pool name as 'current'.
    for (int j=1; j < items; j++) {
      auto& next = listOfTopNs[j];
      // Check on the pool name
      if (getName(current) == getName(next)) indices.push_back(j);
    }

    // Process a new group of items with each's entry index contained in
    // 'indices'. All of them are in the same pool.
    AppendHeavyMemoryQueriesForAPoolInHostAtIndices(ss, listOfTopNs, indices, indent+3);
    // Remove elements just processed.
    for (int i = indices.size() - 1; i >= 0; i--) {
      listOfTopNs.erase(listOfTopNs.begin() + indices[i]);
    }
    // The number of items remaining in the list.
    items = listOfTopNs.size();
  }
  return ss.str();
}

// Return a string reporting top 5 queries with most memory consumed among all
// hosts in a pool.
// Here is one example.
//      topN_query_stats:
//         queries=[
//            id=0000000200000002:0000000000000001, consumed=20.00 MB,
//            id=0000000200000002:0000000000000004, consumed=18.00 MB,
//            id=0000000100000002:0000000000000000, consumed=18.00 MB,
//            id=0000000100000002:0000000000000001, consumed=12.00 MB,
//            id=0000000200000002:0000000000000002, consumed=9.00 MB
//         ],
//         total_consumed=77.00 MB
//         fraction_of_pool_total_mem=0.6
string AdmissionController::GetLogStringForTopNQueriesInPool(
    const std::string& pool_name) {
  // All stats in pool_stats are the starting point to collect top N queries.
  PoolStats* pool_stats = GetPoolStats(pool_name, true);

  std::vector<Item> listOfTopNs;

  // Collect for local stats
  const TPoolStats& local = pool_stats->local_stats();
  for (auto& query : local.heavy_memory_queries) {
    listOfTopNs.emplace_back(
      Item(query.memory_consumed, host_id_, query.queryId, nullptr));
  }

  // Collect for all remote stats
  for (auto& it : pool_stats->remote_stats()) {
    const TPoolStats& remote_stats = it.second;
    for (auto& query : remote_stats.heavy_memory_queries) {
      listOfTopNs.emplace_back(
          Item(query.memory_consumed, it.first /*host id*/, query.queryId, nullptr));
    }
  }

  // If the number of items is 0, no need to go any further.
  if (listOfTopNs.size() == 0) return "";

  // Group items by queryId.
  sort(listOfTopNs.begin(), listOfTopNs.end(), [&](const Item& lhs, const Item& rhs) {
    return getTUniqueId(lhs) < getTUniqueId(rhs);
  });
  // Compute the total mem consumed by all these queries.
  int64_t init_value = 0;
  int64_t total_mem_consumed = std::accumulate(listOfTopNs.begin(), listOfTopNs.end(),
      init_value, [&](auto sum, const auto& x) { return sum + getMemConsumed(x); });
  // Next aggregate on mem_consumed for each group. First define a list of
  // items that will receive the aggregates.
  std::vector<Item> listOfAggregatedItems;
  auto it = listOfTopNs.begin();
  while (it != listOfTopNs.end()) {
    // Find a span of items identical in queryId. The span is defined by [it, next)
    auto next = it;
    next++;
    while (next != listOfTopNs.end() && getTUniqueId(*it) == getTUniqueId(*next)) {
      next++;
    }
    // Aggregate over mem_consumed for items in the span.
    init_value = 0;
    auto sum_mem_consumed = std::accumulate(it, next, init_value,
        [&](auto sum, const auto& x) { return sum + getMemConsumed(x); });
    // Append a new Item at the end of listOfAggregatedItems.
    listOfAggregatedItems.emplace_back(
        Item(sum_mem_consumed, pool_name, getTUniqueId(*it), nullptr));
    // Advance 'it' to possibly start a new span
    it = next;
  }
  // Sort the list in descending order of memory consumed and queryId.
  sort(listOfAggregatedItems.begin(), listOfAggregatedItems.end(),
      std::greater<Item>());
  // Decide the number of topN items to report from the list
  int items = (listOfAggregatedItems.size() >= 5) ?
      5 :
      listOfAggregatedItems.size();
  // Keep first 'items' items and remove the rest.
  listOfAggregatedItems.resize(items);
  // Now we are ready to report the stats.
  // Prepare an index object that indicates the reporting for all elements.
  std::vector<int> indices;
  indices.reserve(items);
  for (int i=0; i<items; i++) indices.emplace_back(i);
  int indent = 0;
  stringstream ss;
  // Report the title.
  OutputIndentedString(ss, indent, "", true);
  OutputIndentedString(
      ss, indent, std::string("Aggregated stats for pool ") + pool_name + ":");
  // Report the topN aggregated queries.
  indent += 3;
  ReportTopNQueriesAtIndices(
      ss, listOfAggregatedItems, indices, indent, total_mem_consumed);
  return ss.str();
}

// Report the topN queries section in a string and append it to 'ss'.
void AdmissionController::ReportTopNQueriesAtIndices(stringstream& ss,
    std::vector<Item>& listOfTopNs, std::vector<int>& indices, int indent,
    int64_t total_mem_consumed) const {
  OutputIndentedString(ss, indent, "topN_query_stats: ");
  indent += 3;
  OutputIndentedString(ss, indent, "queries=[");
  int items = indices.size();
  int64_t total_mem_consumed_by_top_queries = 0;
  indent += 3;
  for (int i = 0; i < items; i++) {
    // Fields in item: memory_consumed, name, queryId, &TPoolStats
    const Item& item = listOfTopNs[indices[i]];
    total_mem_consumed_by_top_queries += getMemConsumed(item);
    // Print queryId.
    OutputIndentedString(ss, indent, "id=", false);
    ss << PrintId(getTUniqueId(item));
    // Print mem consumed.
    ss << ", consumed=" << PrintBytes(getMemConsumed(item));
    if (i < items - 1) ss << ", ";
    ss << std::endl;
  }
  indent -= 3;
  OutputIndentedString(ss, indent, "],");
  OutputIndentedString(ss, indent,
      std::string("total_consumed=")
          + PrintBytes(total_mem_consumed_by_top_queries));

  // Lastly report the percentage of the total.
  if ( total_mem_consumed > 0 ) {
    stringstream local_ss;
    local_ss << setprecision(2)
             << (float)(total_mem_consumed_by_top_queries)
            / total_mem_consumed;
    OutputIndentedString(
        ss, indent, std::string("fraction_of_pool_total_mem=") + local_ss.str());
  }
}

// Append a new string to 'ss' describing queries running in a pool on
// a host:
//  1. The pool name;
//  2. The top-N queries with most memory consumptions among these queries;
//  3. Statistics about all queries
void AdmissionController::AppendHeavyMemoryQueriesForAPoolInHostAtIndices(
    stringstream& ss, std::vector<Item>& listOfTopNs, std::vector<int>& indices,
    int indent) const {
  DCHECK_GT(indices.size(), 0);
  const Item& first_item = listOfTopNs[indices[0]];
  const string& pool_name = getName(first_item);
  // Report the pool name.
  OutputIndentedString(ss, indent, std::string("pool_name=") + pool_name + ": ");
  // Report topN queries.
  indent += 3;
  const TPoolStats* tpool_stats = getTPoolStats(first_item);
  int64_t total_mem_consumed = getTPoolStats(first_item)->total_memory_consumed;
  ReportTopNQueriesAtIndices(
      ss, listOfTopNs, indices, indent, total_mem_consumed);

  // Report stats about all queries
  OutputIndentedString(ss, indent, "all_query_stats: ");
  indent += 3;
  OutputIndentedString(ss, indent, "num_running=", false);
  ss << tpool_stats->num_running << ", " << std::endl;

  OutputIndentedString(ss, indent, "min=", false);
  ss << PrintBytes(tpool_stats->min_memory_consumed) << ", " << std::endl;

  OutputIndentedString(ss, indent, "max=", false);
  ss << PrintBytes(tpool_stats->max_memory_consumed) << ", " << std::endl;

  OutputIndentedString(ss, indent, "pool_total_mem=", false);
  ss << PrintBytes(total_mem_consumed) << ", " << std::endl;
  if (tpool_stats->num_running > 0) {
    OutputIndentedString(ss, indent, "average=", false);
    ss << PrintBytes(total_mem_consumed / tpool_stats->num_running)
       << std::endl;
  }
}

// TODO: do we need host_id_ to come from host_addr or can it just take the same id
// the Scheduler has (coming from the StatestoreSubscriber)?
AdmissionController::AdmissionController(ClusterMembershipMgr* cluster_membership_mgr,
    StatestoreSubscriber* subscriber, RequestPoolService* request_pool_service,
    MetricGroup* metrics, Scheduler* scheduler, PoolMemTrackerRegistry* pool_mem_trackers,
    const TNetworkAddress& host_addr)
  : cluster_membership_mgr_(cluster_membership_mgr),
    subscriber_(subscriber),
    request_pool_service_(request_pool_service),
    metrics_group_(metrics->GetOrCreateChildGroup("admission-controller")),
    scheduler_(scheduler),
    pool_mem_trackers_(pool_mem_trackers),
    host_id_(TNetworkAddressToString(host_addr)),
    thrift_serializer_(false),
    done_(false) {
  cluster_membership_mgr_->RegisterUpdateCallbackFn(
      [this](ClusterMembershipMgr::SnapshotPtr snapshot) {
        this->UpdateExecGroupMetricMap(snapshot);
      });
  total_dequeue_failed_coordinator_limited_ =
      metrics_group_->AddCounter(TOTAL_DEQUEUE_FAILED_COORDINATOR_LIMITED, 0);
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
    pending_dequeue_ = true;
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
  // The executor only needs to read the entry with the key prefix "POOL:" from the topic.
  // This can effectively reduce the network load of the statestore.
  string filter_prefix =
      FLAGS_is_executor && !FLAGS_is_coordinator ? TOPIC_KEY_POOL_PREFIX : "";
  Status status = subscriber_->AddTopic(Statestore::IMPALA_REQUEST_QUEUE_TOPIC,
      /* is_transient=*/true, /* populate_min_subscriber_topic_version=*/false,
      filter_prefix, cb);
  if (!status.ok()) {
    status.AddDetail("AdmissionController failed to register request queue topic");
  }
  return status;
}

void AdmissionController::PoolStats::AdmitQueryAndMemory(
    const ScheduleState& state, bool is_trivial) {
  int64_t cluster_mem_admitted = state.GetClusterMemoryToAdmit();
  DCHECK_GT(cluster_mem_admitted, 0);
  local_mem_admitted_ += cluster_mem_admitted;
  metrics_.local_mem_admitted->Increment(cluster_mem_admitted);

  agg_num_running_ += 1;
  metrics_.agg_num_running->Increment(1L);

  local_stats_.num_admitted_running += 1;
  metrics_.local_num_admitted_running->Increment(1L);

  metrics_.total_admitted->Increment(1L);
  if (is_trivial) ++local_trivial_running_;
}

void AdmissionController::PoolStats::ReleaseQuery(
    int64_t peak_mem_consumption, bool is_trivial) {
  // Update stats tracking the number of running and admitted queries.
  agg_num_running_ -= 1;
  metrics_.agg_num_running->Increment(-1L);

  local_stats_.num_admitted_running -= 1;
  metrics_.local_num_admitted_running->Increment(-1L);

  metrics_.total_released->Increment(1L);
  DCHECK_GE(local_stats_.num_admitted_running, 0);
  DCHECK_GE(agg_num_running_, 0);
  if (is_trivial) {
    --local_trivial_running_;
    DCHECK_GE(local_trivial_running_, 0);
  }

  // Update the 'peak_mem_histogram' based on the given peak memory consumption of the
  // query, if provided.
  if (peak_mem_consumption != -1) {
    int64_t histogram_bucket =
        BitUtil::RoundUp(peak_mem_consumption, HISTOGRAM_BIN_SIZE) / HISTOGRAM_BIN_SIZE;
    histogram_bucket =
        std::max(std::min(histogram_bucket, HISTOGRAM_NUM_OF_BINS), 1L) - 1;
    peak_mem_histogram_[histogram_bucket] = ++(peak_mem_histogram_[histogram_bucket]);
  }
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

void AdmissionController::UpdateStatsOnReleaseForBackends(const UniqueIdPB& query_id,
    RunningQuery& running_query, const vector<NetworkAddressPB>& host_addrs) {
  int64_t total_mem_to_release = 0;
  for (const auto& host_addr : host_addrs) {
    auto backend_allocation = running_query.per_backend_resources.find(host_addr);
    if (backend_allocation == running_query.per_backend_resources.end()) {
      // In the context of the admission control service, this may happen, eg. if a
      // ReleaseQueryBackends rpc is delayed in the network and arrives after the
      // ReleaseQuery rpc, so only log as a WARNING.
      string err_msg =
          strings::Substitute("Error: Cannot find exec params of host $0 for query $1.",
              NetworkAddressPBToString(host_addr), PrintId(query_id));
      LOG(WARNING) << err_msg;
      continue;
    }
    UpdateHostStats(host_addr, -backend_allocation->second.mem_to_admit, -1,
        -backend_allocation->second.slots_to_use);
    total_mem_to_release += backend_allocation->second.mem_to_admit;
    running_query.per_backend_resources.erase(backend_allocation);
  }
  PoolStats* pool_stats = GetPoolStats(running_query.request_pool);
  pool_stats->ReleaseMem(total_mem_to_release);
  pools_for_updates_.insert(running_query.request_pool);
}

void AdmissionController::UpdateStatsOnAdmission(
    const ScheduleState& state, bool is_trivial) {
  for (const auto& entry : state.per_backend_schedule_states()) {
    const NetworkAddressPB& host_addr = entry.first;
    int64_t mem_to_admit = GetMemToAdmit(state, entry.second);
    UpdateHostStats(host_addr, mem_to_admit, 1, entry.second.exec_params->slots_to_use());
  }
  PoolStats* pool_stats = GetPoolStats(state);
  pool_stats->AdmitQueryAndMemory(state, is_trivial);
  pools_for_updates_.insert(state.request_pool());
}

void AdmissionController::UpdateHostStats(const NetworkAddressPB& host_addr,
    int64_t mem_to_admit, int num_queries_to_admit, int num_slots_to_admit) {
  const string host = NetworkAddressPBToString(host_addr);
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
  VLOG_ROW << "Update slots in use for host=" << host
           << " prev=" << host_stats_[host].slots_in_use
           << " new=" << host_stats_[host].slots_in_use + num_slots_to_admit;
  host_stats_[host].slots_in_use += num_slots_to_admit;
  DCHECK_GE(host_stats_[host].slots_in_use, 0);
}

// Helper method used by CanAccommodateMaxInitialReservation(). Returns true if the given
// 'mem_limit' can accommodate 'buffer_reservation'. If not, returns false and the
// details about the memory shortage in 'mem_unavailable_reason'.
static bool CanMemLimitAccommodateReservation(const int64_t mem_limit,
    const MemLimitSourcePB mem_limit_source, const int64_t buffer_reservation,
    const string& request_pool, string* mem_unavailable_reason) {
  if (mem_limit <= 0) return true; // No mem limit.
  const int64_t max_reservation =
      ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
  if (buffer_reservation <= max_reservation) return true;
  const int64_t required_mem_limit =
      ReservationUtil::GetMinMemLimitFromReservation(buffer_reservation);
  string config_name = "<config_name>";
  switch (mem_limit_source) {
    case MemLimitSourcePB::NO_LIMIT:
      DCHECK(false) << "MemLimitSourcePB::NO_LIMIT only valid for mem_limit <= 0";
      break;
    case MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT:
    case MemLimitSourcePB::QUERY_PLAN_PER_HOST_MEM_ESTIMATE:
    case MemLimitSourcePB::ADJUSTED_PER_HOST_MEM_ESTIMATE:
    case MemLimitSourcePB::QUERY_PLAN_DEDICATED_COORDINATOR_MEM_ESTIMATE:
    case MemLimitSourcePB::ADJUSTED_DEDICATED_COORDINATOR_MEM_ESTIMATE:
      config_name = to_string(TImpalaQueryOptions::MEM_LIMIT) + " option";
      break;
    case MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT_EXECUTORS:
      config_name = to_string(TImpalaQueryOptions::MEM_LIMIT_EXECUTORS) + " option";
      break;
    case MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT_COORDINATORS:
      config_name = to_string(TImpalaQueryOptions::MEM_LIMIT_COORDINATORS) + " option";
      break;
    case MemLimitSourcePB::POOL_CONFIG_MIN_QUERY_MEM_LIMIT:
      config_name =
          Substitute("impala.admission-control.min-query-mem-limit of request pool '$0'",
              request_pool);
      break;
    case MemLimitSourcePB::POOL_CONFIG_MAX_QUERY_MEM_LIMIT:
      config_name =
          Substitute("impala.admission-control.max-query-mem-limit of request pool '$0'",
              request_pool);
      break;
    case MemLimitSourcePB::HOST_MEM_TRACKER_LIMIT:
      config_name = "system memory or the CGroup memory limit";
      break;
    case MemLimitSourcePB::COORDINATOR_ONLY_OPTIMIZATION:
      DCHECK(false) << "Coordinator only query should have mem_limit == 0";
      break;
    default:
      DCHECK(false) << "Unknown MemLimitSourcePB enum: " << mem_limit_source;
  }
  *mem_unavailable_reason = Substitute(REASON_MEM_LIMIT_TOO_LOW_FOR_RESERVATION,
      PrintBytes(buffer_reservation), config_name, PrintBytes(required_mem_limit),
      Scheduler::PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION);
  return false;
}

bool AdmissionController::CanAccommodateMaxInitialReservation(const ScheduleState& state,
    const TPoolConfig& pool_cfg, string* mem_unavailable_reason) {
  // Executors mem_limit.
  const int64_t executor_mem_limit = state.per_backend_mem_limit();
  const int64_t executor_min_reservation = state.largest_min_reservation();
  if (executor_mem_limit > 0) {
    DCHECK_EQ(executor_mem_limit, state.per_backend_mem_to_admit());
  }
  // Coordinator mem_limit.
  const int64_t coord_mem_limit = state.coord_backend_mem_limit();
  const int64_t coord_min_reservation = state.coord_min_reservation();
  if (coord_mem_limit > 0) {
    DCHECK_EQ(coord_mem_limit, state.coord_backend_mem_to_admit());
  }
  return CanMemLimitAccommodateReservation(executor_mem_limit,
             state.per_backend_mem_to_admit_source(), executor_min_reservation,
             state.request_pool(), mem_unavailable_reason)
      && CanMemLimitAccommodateReservation(coord_mem_limit,
          state.coord_backend_mem_to_admit_source(), coord_min_reservation,
          state.request_pool(), mem_unavailable_reason);
}

bool AdmissionController::HasAvailableMemResources(const ScheduleState& state,
    const TPoolConfig& pool_cfg, string* mem_unavailable_reason,
    bool& coordinator_resource_limited, string* not_admitted_details) {
  const string& pool_name = state.request_pool();
  const int64_t pool_max_mem = GetMaxMemForPool(pool_cfg);
  // If the pool doesn't have memory resources configured, always true.
  if (pool_max_mem < 0) return true;

  // Otherwise, two conditions must be met:
  // 1) The memory estimated to be reserved by all queries in this pool *plus* the total
  //    memory needed for this query must be within the max pool memory resources
  //    specified.
  // 2) Each individual backend must have enough mem available within its process limit
  //    to execute the query.

  // Case 1:
  PoolStats* stats = GetPoolStats(state);
  int64_t cluster_mem_to_admit = state.GetClusterMemoryToAdmit();
  VLOG_RPC << "Checking agg mem in pool=" << pool_name << " : " << stats->DebugString()
           << " executor_group=" << state.executor_group()
           << " cluster_mem_needed=" << PrintBytes(cluster_mem_to_admit)
           << " pool_max_mem=" << PrintBytes(pool_max_mem);
  if (stats->EffectiveMemReserved() + cluster_mem_to_admit > pool_max_mem) {
    *mem_unavailable_reason = Substitute(POOL_MEM_NOT_AVAILABLE, pool_name,
        PrintBytes(pool_max_mem), PrintBytes(cluster_mem_to_admit),
        PrintBytes(max(pool_max_mem - stats->EffectiveMemReserved(), 0L)),
        GetStalenessDetailLocked(" "));
    // Find info about the top-N queries with most memory consumption from both
    // local and remote stats in this pool.
    if ( not_admitted_details ) {
      *not_admitted_details = GetLogStringForTopNQueriesInPool(pool_name);
    }
    return false;
  }

  // Case 2:
  for (const auto& entry : state.per_backend_schedule_states()) {
    const NetworkAddressPB& host = entry.first;
    const string host_id = NetworkAddressPBToString(host);
    int64_t admit_mem_limit = entry.second.be_desc.admit_mem_limit();
    const THostStats& host_stats = host_stats_[host_id];
    int64_t mem_reserved = host_stats.mem_reserved;
    int64_t agg_mem_admitted_on_host = host_stats.mem_admitted;
    // Aggregate the mem admitted across all queries admitted by other coordinators.
    for (const auto& remote_entry : remote_per_host_stats_) {
      auto remote_stat_itr = remote_entry.second.find(host_id);
      if (remote_stat_itr != remote_entry.second.end()) {
        agg_mem_admitted_on_host += remote_stat_itr->second.mem_admitted;
      }
    }
    int64_t mem_to_admit = GetMemToAdmit(state, entry.second);
    VLOG_ROW << "Checking memory on host=" << host_id
             << " mem_reserved=" << PrintBytes(mem_reserved)
             << " mem_admitted=" << PrintBytes(host_stats.mem_admitted)
             << " agg_mem_admitted_on_host=" << PrintBytes(agg_mem_admitted_on_host)
             << " needs=" << PrintBytes(mem_to_admit)
             << " admit_mem_limit=" << PrintBytes(admit_mem_limit);
    int64_t effective_host_mem_reserved =
        std::max(mem_reserved, agg_mem_admitted_on_host);
    if (effective_host_mem_reserved + mem_to_admit > admit_mem_limit) {
      *mem_unavailable_reason =
          Substitute(HOST_MEM_NOT_AVAILABLE, host_id, PrintBytes(mem_to_admit),
              PrintBytes(max(admit_mem_limit - effective_host_mem_reserved, 0L)),
              PrintBytes(admit_mem_limit), GetStalenessDetailLocked(" "));
      // Find info about the top-N queries with most memory consumption from all
      // pools at this host.
      if ( not_admitted_details ) {
        *not_admitted_details = GetLogStringForTopNQueriesOnHost(host_id);
      }
      if (entry.second.be_desc.is_coordinator()) {
        coordinator_resource_limited = true;
      }
      return false;
    }
  }
  const TQueryOptions& query_opts = state.query_options();
  if (!query_opts.__isset.buffer_pool_limit || query_opts.buffer_pool_limit <= 0) {
    // Check if a change in pool_cfg.max_query_mem_limit (while the query was queued)
    // resulted in a decrease in the computed per_host_mem_limit such that it can no
    // longer accommodate the largest min_reservation.
    return CanAccommodateMaxInitialReservation(state, pool_cfg, mem_unavailable_reason);
  }
  return true;
}

bool AdmissionController::HasAvailableSlots(const ScheduleState& state,
    const TPoolConfig& pool_cfg, string* unavailable_reason,
    bool& coordinator_resource_limited) {
  for (const auto& entry : state.per_backend_schedule_states()) {
    const NetworkAddressPB& host = entry.first;
    const string host_id = NetworkAddressPBToString(host);
    int64_t admission_slots = entry.second.be_desc.admission_slots();
    int64_t agg_slots_in_use_on_host = host_stats_[host_id].slots_in_use;
    // Aggregate num of slots in use across all queries admitted by other coordinators.
    for (const auto& remote_entry : remote_per_host_stats_) {
      auto remote_stat_itr = remote_entry.second.find(host_id);
      if (remote_stat_itr != remote_entry.second.end()) {
        agg_slots_in_use_on_host += remote_stat_itr->second.slots_in_use;
      }
    }
    VLOG_ROW << "Checking available slot on host=" << host_id
             << " slots_in_use=" << agg_slots_in_use_on_host << " needs="
             << agg_slots_in_use_on_host + entry.second.exec_params->slots_to_use()
             << " executor admission_slots=" << admission_slots;
    if (agg_slots_in_use_on_host + entry.second.exec_params->slots_to_use()
        > admission_slots) {
      *unavailable_reason = Substitute(HOST_SLOT_NOT_AVAILABLE, host_id,
          entry.second.exec_params->slots_to_use(), agg_slots_in_use_on_host,
          admission_slots);
      if (entry.second.be_desc.is_coordinator()) {
        coordinator_resource_limited = true;
      }
      return false;
    }
  }
  return true;
}

bool AdmissionController::CanAdmitTrivialRequest(const ScheduleState& state) {
  PoolStats* pool_stats = GetPoolStats(state);
  DCHECK(pool_stats != nullptr);
  return pool_stats->local_trivial_running() + 1
      <= PoolStats::MAX_NUM_TRIVIAL_QUERY_RUNNING;
}

bool AdmissionController::CanAdmitRequest(const ScheduleState& state,
    const TPoolConfig& pool_cfg, bool admit_from_queue, string* not_admitted_reason,
    string* not_admitted_details, bool& coordinator_resource_limited) {
  // Can't admit if:
  //  (a) There are already queued requests (and this is not admitting from the queue).
  //  (b) The resource pool is already at the maximum number of requests.
  //  (c) One of the executors or coordinator in 'schedule' is already at its maximum
  //      admission slots (when not using the default executor group).
  //  (d) There are not enough memory resources available for the query.
  const int64_t max_requests = GetMaxRequestsForPool(pool_cfg);
  PoolStats* pool_stats = GetPoolStats(state);
  bool default_group =
      state.executor_group() == ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME;
  bool default_coordinator_only =
      state.executor_group() == ClusterMembershipMgr::EMPTY_GROUP_NAME
      && state.request_pool() == RequestPoolService::DEFAULT_POOL_NAME;
  if (!admit_from_queue && pool_stats->local_stats().num_queued > 0) {
    *not_admitted_reason = Substitute(QUEUED_QUEUE_NOT_EMPTY,
        pool_stats->local_stats().num_queued, GetStalenessDetailLocked(" "));
    return false;
  }
  if (max_requests >= 0 && pool_stats->agg_num_running() >= max_requests) {
    // All executor groups are limited by the aggregate number of queries running in the
    // pool.
    *not_admitted_reason = Substitute(QUEUED_NUM_RUNNING, pool_stats->agg_num_running(),
        max_requests, GetStalenessDetailLocked(" "));
    return false;
  }
  if (!default_group && !default_coordinator_only
      && !HasAvailableSlots(
          state, pool_cfg, not_admitted_reason, coordinator_resource_limited)) {
    // All non-default executor groups are also limited by the number of running queries
    // per executor.
    // TODO(IMPALA-8757): Extend slot based admission to default executor group
    return false;
  }
  if (!HasAvailableMemResources(state, pool_cfg, not_admitted_reason,
          coordinator_resource_limited, not_admitted_details)) {
    return false;
  }
  return true;
}

bool AdmissionController::RejectForCluster(const string& pool_name,
    const TPoolConfig& pool_cfg, bool admit_from_queue, string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());

  // Checks related to pool max_requests:
  if (GetMaxRequestsForPool(pool_cfg) == 0) {
    *rejection_reason = REASON_DISABLED_REQUESTS_LIMIT;
    return true;
  }

  // Checks related to pool max_mem_resources:
  int64_t max_mem = GetMaxMemForPool(pool_cfg);
  if (max_mem == 0) {
    *rejection_reason = REASON_DISABLED_MAX_MEM_RESOURCES;
    return true;
  }

  if (max_mem > 0 && pool_cfg.min_query_mem_limit > max_mem) {
    *rejection_reason = Substitute(REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_MEM,
        pool_cfg.min_query_mem_limit, max_mem);
    return true;
  }

  if (pool_cfg.max_query_mem_limit > 0
      && pool_cfg.min_query_mem_limit > pool_cfg.max_query_mem_limit) {
    *rejection_reason = Substitute(REASON_INVALID_POOL_CONFIG_MIN_LIMIT_MAX_LIMIT,
        pool_cfg.min_query_mem_limit, pool_cfg.max_query_mem_limit);
    return true;
  }

  PoolStats* stats = GetPoolStats(pool_name);
  int64_t max_queued = GetMaxQueuedForPool(pool_cfg);
  if (!admit_from_queue && stats->agg_num_queued() >= max_queued) {
    *rejection_reason = Substitute(REASON_QUEUE_FULL, max_queued, stats->agg_num_queued(),
        GetStalenessDetailLocked(" "));
    return true;
  }

  return false;
}

bool AdmissionController::RejectForSchedule(
    const ScheduleState& state, const TPoolConfig& pool_cfg, string* rejection_reason) {
  DCHECK(rejection_reason != nullptr && rejection_reason->empty());
  bool default_group =
      state.executor_group() == ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME;

  // Compute the max (over all backends), the cluster totals (across all backends) for
  // min_mem_reservation_bytes, thread_reservation, the min admit_mem_limit
  // (over all executors) and the admit_mem_limit of the coordinator.
  pair<const NetworkAddressPB*, int64_t> largest_min_mem_reservation(nullptr, -1);
  int64_t cluster_min_mem_reservation_bytes = 0;
  pair<const NetworkAddressPB*, int64_t> max_thread_reservation(nullptr, 0);
  pair<const NetworkAddressPB*, int64_t> min_executor_admit_mem_limit(
      nullptr, std::numeric_limits<int64_t>::max());
  pair<const NetworkAddressPB*, int64_t> coord_admit_mem_limit(
      nullptr, std::numeric_limits<int64_t>::max());
  int64_t cluster_thread_reservation = 0;
  for (const auto& e : state.per_backend_schedule_states()) {
    const BackendScheduleState& be_state = e.second;
    // TODO(IMPALA-8757): Extend slot based admission to default executor group
    if (!default_group
        && be_state.exec_params->slots_to_use() > be_state.be_desc.admission_slots()) {
      *rejection_reason = Substitute(REASON_NOT_ENOUGH_SLOTS_ON_BACKEND,
          be_state.exec_params->slots_to_use(),
          NetworkAddressPBToString(be_state.be_desc.address()),
          be_state.be_desc.admission_slots());
      return true;
    }

    cluster_min_mem_reservation_bytes +=
        be_state.exec_params->min_mem_reservation_bytes();
    if (be_state.exec_params->min_mem_reservation_bytes()
        > largest_min_mem_reservation.second) {
      largest_min_mem_reservation =
          make_pair(&e.first, be_state.exec_params->min_mem_reservation_bytes());
    }
    cluster_thread_reservation += be_state.exec_params->thread_reservation();
    if (be_state.exec_params->thread_reservation() > max_thread_reservation.second) {
      max_thread_reservation =
          make_pair(&e.first, be_state.exec_params->thread_reservation());
    }
    if (!FLAGS_clamp_query_mem_limit_backend_mem_limit) {
      if (be_state.exec_params->is_coord_backend()) {
        coord_admit_mem_limit.first = &e.first;
        coord_admit_mem_limit.second = be_state.be_desc.admit_mem_limit();
      } else if (be_state.be_desc.admit_mem_limit() <
            min_executor_admit_mem_limit.second) {
        min_executor_admit_mem_limit.first = &e.first;
        min_executor_admit_mem_limit.second = be_state.be_desc.admit_mem_limit();
      }
    }
  }

  // Checks related to the min buffer reservation against configured query memory limits:
  const TQueryOptions& query_opts = state.query_options();
  if (query_opts.__isset.buffer_pool_limit && query_opts.buffer_pool_limit > 0) {
    if (largest_min_mem_reservation.second > query_opts.buffer_pool_limit) {
      *rejection_reason = Substitute(REASON_BUFFER_LIMIT_TOO_LOW_FOR_RESERVATION,
          NetworkAddressPBToString(*largest_min_mem_reservation.first),
          PrintBytes(largest_min_mem_reservation.second),
          Scheduler::PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION);
      return true;
    }
  } else if (!CanAccommodateMaxInitialReservation(state, pool_cfg, rejection_reason)) {
    // If buffer_pool_limit is not explicitly set, it's calculated from mem_limit.
    return true;
  }

  // Check thread reservation limits.
  if (query_opts.__isset.thread_reservation_limit
      && query_opts.thread_reservation_limit > 0
      && max_thread_reservation.second > query_opts.thread_reservation_limit) {
    *rejection_reason = Substitute(REASON_THREAD_RESERVATION_LIMIT_EXCEEDED,
        NetworkAddressPBToString(*max_thread_reservation.first),
        max_thread_reservation.second, query_opts.thread_reservation_limit);
    return true;
  }
  if (query_opts.__isset.thread_reservation_aggregate_limit
      && query_opts.thread_reservation_aggregate_limit > 0
      && cluster_thread_reservation > query_opts.thread_reservation_aggregate_limit) {
    *rejection_reason = Substitute(REASON_THREAD_RESERVATION_AGG_LIMIT_EXCEEDED,
        state.per_backend_schedule_states().size(), cluster_thread_reservation,
        query_opts.thread_reservation_aggregate_limit);
    return true;
  }

  // Checks related to pool max_mem_resources:
  // We perform these checks here against the group_size to prevent queuing up queries
  // that would never be able to reserve the required memory on an executor group.
  int64_t max_mem = GetMaxMemForPool(pool_cfg);
  if (max_mem == 0) {
    *rejection_reason = REASON_DISABLED_MAX_MEM_RESOURCES;
    return true;
  }
  if (max_mem > 0) {
    if (cluster_min_mem_reservation_bytes > max_mem) {
      *rejection_reason = Substitute(REASON_MIN_RESERVATION_OVER_POOL_MEM,
          PrintBytes(max_mem), PrintBytes(cluster_min_mem_reservation_bytes),
          Scheduler::PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION);
      return true;
    }
    int64_t cluster_mem_to_admit = state.GetClusterMemoryToAdmit();
    if (cluster_mem_to_admit > max_mem) {
      *rejection_reason = Substitute(REASON_REQ_OVER_POOL_MEM,
          PrintBytes(cluster_mem_to_admit), PrintBytes(max_mem));
      return true;
    }
    if (!FLAGS_clamp_query_mem_limit_backend_mem_limit) {
      int64_t executor_mem_to_admit = state.per_backend_mem_to_admit();
      VLOG_ROW << "Checking executor mem with executor_mem_to_admit = "
               << executor_mem_to_admit
               << " and min_admit_mem_limit.second = "
               << min_executor_admit_mem_limit.second;
      if (executor_mem_to_admit > min_executor_admit_mem_limit.second) {
        *rejection_reason =
            Substitute(REASON_REQ_OVER_NODE_MEM, PrintBytes(executor_mem_to_admit),
                PrintBytes(min_executor_admit_mem_limit.second),
                NetworkAddressPBToString(*min_executor_admit_mem_limit.first));
        return true;
      }
      int64_t coord_mem_to_admit = state.coord_backend_mem_to_admit();
      VLOG_ROW << "Checking coordinator mem with coord_mem_to_admit = "
               << coord_mem_to_admit
               << " and coord_admit_mem_limit.second = " << coord_admit_mem_limit.second;
      if (coord_mem_to_admit > coord_admit_mem_limit.second) {
        *rejection_reason = Substitute(REASON_REQ_OVER_NODE_MEM,
            PrintBytes(coord_mem_to_admit), PrintBytes(coord_admit_mem_limit.second),
            NetworkAddressPBToString(*coord_admit_mem_limit.first));
        return true;
      }
    }
  }
  return false;
}

void AdmissionController::PoolStats::UpdateConfigMetrics(const TPoolConfig& pool_cfg) {
  metrics_.pool_max_mem_resources->SetValue(pool_cfg.max_mem_resources);
  metrics_.pool_max_requests->SetValue(pool_cfg.max_requests);
  metrics_.pool_max_queued->SetValue(pool_cfg.max_queued);
  metrics_.pool_queue_timeout->SetValue(GetQueueTimeoutForPoolMs(pool_cfg));
  metrics_.max_query_mem_limit->SetValue(pool_cfg.max_query_mem_limit);
  metrics_.min_query_mem_limit->SetValue(pool_cfg.min_query_mem_limit);
  metrics_.clamp_mem_limit_query_option->SetValue(pool_cfg.clamp_mem_limit_query_option);
  metrics_.max_query_cpu_core_per_node_limit->SetValue(
      pool_cfg.max_query_cpu_core_per_node_limit);
  metrics_.max_query_cpu_core_coordinator_limit->SetValue(
      pool_cfg.max_query_cpu_core_coordinator_limit);
}

Status AdmissionController::SubmitForAdmission(const AdmissionRequest& request,
    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admit_outcome,
    unique_ptr<QuerySchedulePB>* schedule_result, bool& queued,
    std::string* request_pool) {
  queued = false;
  DebugActionNoFail(request.query_options, "AC_BEFORE_ADMISSION");
  DCHECK(schedule_result->get() == nullptr);

  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr_->GetSnapshot();
  DCHECK(membership_snapshot.get() != nullptr);
  string blacklist_str = membership_snapshot->executor_blacklist.BlacklistToString();
  if (!blacklist_str.empty()) {
    request.summary_profile->AddInfoString("Blacklisted Executors", blacklist_str);
  }

  QueueNode* queue_node;
  {
    lock_guard<mutex> lock(queue_nodes_lock_);
    auto it = queue_nodes_.emplace(std::piecewise_construct,
        std::forward_as_tuple(request.query_id),
        std::forward_as_tuple(request, admit_outcome, request.summary_profile));
    if (!it.second) {
      // The query_id already existed in queue_nodes_.
      return Status("Cannot submit the same query for admission multiple times.");
    }
    queue_node = &it.first->second;
  }

  const auto queue_node_deleter = MakeScopeExitTrigger([&]() {
    if (!queued) {
      lock_guard<mutex> lock(queue_nodes_lock_);
      queue_nodes_.erase(request.query_id);
    }
  });

  // Re-resolve the pool name to propagate any resolution errors now that this request is
  // known to require a valid pool. All executor groups / schedules will use the same pool
  // name.
  RETURN_IF_ERROR(ResolvePoolAndGetConfig(
      request.request.query_ctx, &queue_node->pool_name, &queue_node->pool_cfg));
  request.summary_profile->AddInfoString("Request Pool", queue_node->pool_name);

  {
    // Take lock to ensure the Dequeue thread does not modify the request queue.
    lock_guard<mutex> lock(admission_ctrl_lock_);

    pool_config_map_[queue_node->pool_name] = queue_node->pool_cfg;
    PoolStats* stats = GetPoolStats(queue_node->pool_name);
    stats->UpdateConfigMetrics(queue_node->pool_cfg);

    bool unused_bool;
    bool is_trivial = false;
    bool must_reject =
        !FindGroupToAdmitOrReject(membership_snapshot, queue_node->pool_cfg,
            /* admit_from_queue=*/false, stats, queue_node, unused_bool, &is_trivial);
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
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_REJECTED,
          queue_node->pool_name, queue_node->not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    }

    if (queue_node->admitted_schedule.get() != nullptr) {
      DCHECK(queue_node->admitted_schedule->query_schedule_pb().get() != nullptr);
      const string& group_name = queue_node->admitted_schedule->executor_group();
      VLOG(3) << "Can admit to group " << group_name << " (or cancelled)";
      if (!is_trivial) DCHECK_EQ(stats->local_stats().num_queued, 0);
      AdmissionOutcome outcome = admit_outcome->Set(AdmissionOutcome::ADMITTED);
      if (outcome != AdmissionOutcome::ADMITTED) {
        DCHECK_ENUM_EQ(outcome, AdmissionOutcome::CANCELLED);
        VLOG_QUERY << "Ready to be "
                   << (is_trivial ? PROFILE_INFO_VAL_ADMIT_TRIVIAL :
                                    PROFILE_INFO_VAL_ADMIT_IMMEDIATELY)
                   << " but already cancelled, query id=" << PrintId(request.query_id);
        return Status::CANCELLED;
      }
      VLOG_QUERY << "Admitting query id=" << PrintId(request.query_id);
      AdmitQuery(queue_node, false /* was_queued */, is_trivial);
      stats->UpdateWaitTime(0);
      VLOG_RPC << "Final: " << stats->DebugString();
      *schedule_result = move(queue_node->admitted_schedule->query_schedule_pb());
      if (request_pool != nullptr) *request_pool = queue_node->pool_name;
      return Status::OK();
    }

    // We cannot immediately admit but do not need to reject, so queue the request
    RequestQueue* queue = &request_queue_map_[queue_node->pool_name];
    VLOG_QUERY << "Queuing, query id=" << PrintId(request.query_id)
               << " reason: " << queue_node->not_admitted_reason;
    if (queue_node->not_admitted_details.size() > 0) {
      VLOG_RPC << "Top mem consuming queries: " << queue_node->not_admitted_details;
    }
    queue_node->initial_queue_reason = queue_node->not_admitted_reason;
    stats->Queue();
    queue->Enqueue(queue_node);

    // Must be done while we still hold 'admission_ctrl_lock_' as the dequeue loop thread
    // can modify 'not_admitted_reason'.
    request.summary_profile->AddInfoString(
        PROFILE_INFO_KEY_LAST_QUEUED_REASON, queue_node->not_admitted_reason);
  }

  // Update the profile info before waiting. These properties will be updated with
  // their final state after being dequeued.
  request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_QUEUED);
  request.summary_profile->AddInfoString(
      PROFILE_INFO_KEY_INITIAL_QUEUE_REASON, queue_node->initial_queue_reason);

  queue_node->wait_start_ms = MonotonicMillis();
  queued = true;
  return Status::OK();
}

Status AdmissionController::WaitOnQueued(const UniqueIdPB& query_id,
    unique_ptr<QuerySchedulePB>* schedule_result, int64_t timeout_ms,
    bool* wait_timed_out, int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) {
  if (wait_timed_out != nullptr) *wait_timed_out = false;
  if (wait_start_time_ms != nullptr) *wait_start_time_ms = 0;
  if (wait_end_time_ms != nullptr) *wait_end_time_ms = 0;

  QueueNode* queue_node;
  {
    lock_guard<mutex> lock(queue_nodes_lock_);
    auto it = queue_nodes_.find(query_id);
    if (it == queue_nodes_.end()) {
      return Status(
          Substitute("WaitOnQueued failed: unknown query_id=$0", PrintId(query_id)));
    }
    queue_node = &it->second;
  }

  if (wait_start_time_ms != nullptr) *wait_start_time_ms = queue_node->wait_start_ms;

  int64_t queue_wait_timeout_ms = GetQueueTimeoutForPoolMs(queue_node->pool_cfg);

  // Block in Get() up to the time out, waiting for the promise to be set when the query
  // is admitted or cancelled.
  bool get_timed_out = false;
  queue_node->admit_outcome->Get(
      (timeout_ms > 0 ? min(queue_wait_timeout_ms, timeout_ms) : queue_wait_timeout_ms),
      &get_timed_out);
  int64_t wait_end_ms = MonotonicMillis();
  int64_t wait_time_ms = wait_end_ms - queue_node->wait_start_ms;

  queue_node->profile->AddInfoString(PROFILE_INFO_KEY_INITIAL_QUEUE_REASON,
      Substitute(PROFILE_INFO_VAL_INITIAL_QUEUE_REASON, wait_time_ms,
          queue_node->initial_queue_reason));

  if (get_timed_out && wait_time_ms < queue_wait_timeout_ms) {
    if (wait_timed_out != nullptr) *wait_timed_out = true;
    // No admission decision has been made yet, so just return.
    return Status::OK();
  }

  if (wait_end_time_ms != nullptr) *wait_end_time_ms = wait_end_ms;

  const auto queue_node_deleter = MakeScopeExitTrigger([&]() {
    lock_guard<mutex> lock(queue_nodes_lock_);
    queue_nodes_.erase(query_id);
  });

  // Disallow the FAIL action here. It would leave the queue in an inconsistent state.
  DebugActionNoFail(
      queue_node->admission_request.query_options, "AC_AFTER_ADMISSION_OUTCOME");

  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    // If the query has not been admitted or cancelled up till now, it will be considered
    // to be timed out.
    AdmissionOutcome outcome =
        queue_node->admit_outcome->Set(AdmissionOutcome::TIMED_OUT);
    RequestQueue* queue = &request_queue_map_[queue_node->pool_name];
    pools_for_updates_.insert(queue_node->pool_name);
    PoolStats* pool_stats = GetPoolStats(queue_node->pool_name);
    pool_stats->UpdateWaitTime(wait_time_ms);
    if (outcome == AdmissionOutcome::REJECTED) {
      if (queue->Remove(queue_node)) pool_stats->Dequeue(true);
      queue_node->profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_REJECTED);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_REJECTED,
          queue_node->pool_name, queue_node->not_admitted_reason);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    } else if (outcome == AdmissionOutcome::TIMED_OUT) {
      bool removed = queue->Remove(queue_node);
      DCHECK(removed);
      pool_stats->Dequeue(true);
      queue_node->profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_TIME_OUT);
      const ErrorMsg& rejected_msg = ErrorMsg(TErrorCode::ADMISSION_TIMED_OUT,
          queue_wait_timeout_ms, queue_node->pool_name, queue_node->not_admitted_reason,
          queue_node->not_admitted_details);
      VLOG_QUERY << rejected_msg.msg();
      return Status::Expected(rejected_msg);
    } else if (outcome == AdmissionOutcome::CANCELLED) {
      if (queue->Remove(queue_node)) {
        pool_stats->Dequeue(false);
      }
      queue_node->profile->AddInfoString(
          PROFILE_INFO_KEY_ADMISSION_RESULT, PROFILE_INFO_VAL_CANCELLED_IN_QUEUE);
      VLOG_QUERY << PROFILE_INFO_VAL_CANCELLED_IN_QUEUE
                 << ", query id=" << PrintId(query_id);
      return Status::CANCELLED;
    }
    // The dequeue thread updates the stats (to avoid a race condition) so we do
    // not change them here.
    DCHECK_ENUM_EQ(outcome, AdmissionOutcome::ADMITTED);
    DCHECK(queue_node->admitted_schedule.get() != nullptr);
    *schedule_result = move(queue_node->admitted_schedule->query_schedule_pb());
    DCHECK(!queue->Contains(queue_node));
    VLOG_QUERY << "Admitted queued query id=" << PrintId(query_id);
    VLOG_RPC << "Final: " << pool_stats->DebugString();
    return Status::OK();
  }
}

void AdmissionController::ReleaseQuery(const UniqueIdPB& query_id,
    const UniqueIdPB& coord_id, int64_t peak_mem_consumption,
    bool release_remaining_backends) {
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    auto host_it = running_queries_.find(coord_id);
    if (host_it == running_queries_.end()) {
      // In the context of the admission control service, this may happen, eg. if a
      // coordinator is reported as failed by the statestore but a ReleaseQuery rpc from
      // it is delayed in the network and arrives much later.
      LOG(WARNING) << "Unable to find host " << PrintId(coord_id)
                   << " to get resources to release for query " << PrintId(query_id)
                   << ", may have already been released.";
      return;
    }
    auto it = host_it->second.find(query_id);
    if (it == host_it->second.end()) {
      // In the context of the admission control service, this may happen, eg. if a
      // ReleaseQuery rpc is reported as failed to the coordinator but actually ends up
      // arriving much later, so only log at WARNING level.
      LOG(WARNING) << "Unable to find resources to release for query "
                   << PrintId(query_id) << ", may have already been released.";
      return;
    }

    const RunningQuery& running_query = it->second;
    if (release_remaining_backends) {
      vector<NetworkAddressPB> to_release;
      for (const auto& entry : running_query.per_backend_resources) {
        to_release.push_back(entry.first);
      }
      if (to_release.size() > 0) {
        LOG(INFO) << "ReleaseQuery for " << query_id << " called with "
                  << to_release.size()
                  << "unreleased backends. Releasing automatically.";
        ReleaseQueryBackendsLocked(query_id, coord_id, to_release);
      }
    }
    DCHECK_EQ(num_released_backends_.at(query_id), 0) << PrintId(query_id);
    num_released_backends_.erase(num_released_backends_.find(query_id));
    PoolStats* stats = GetPoolStats(running_query.request_pool);
    stats->ReleaseQuery(peak_mem_consumption, running_query.is_trivial);
    // No need to update the Host Stats as they should have been updated in
    // ReleaseQueryBackends.
    pools_for_updates_.insert(running_query.request_pool);
    UpdateExecGroupMetric(running_query.executor_group, -1);
    VLOG_RPC << "Released query id=" << PrintId(query_id) << " " << stats->DebugString();
    pending_dequeue_ = true;
    host_it->second.erase(it);
  }
  dequeue_cv_.NotifyOne();
}

void AdmissionController::ReleaseQueryBackends(const UniqueIdPB& query_id,
    const UniqueIdPB& coord_id, const vector<NetworkAddressPB>& host_addrs) {
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    ReleaseQueryBackendsLocked(query_id, coord_id, host_addrs);
  }
  dequeue_cv_.NotifyOne();
}

void AdmissionController::ReleaseQueryBackendsLocked(const UniqueIdPB& query_id,
    const UniqueIdPB& coord_id, const vector<NetworkAddressPB>& host_addrs) {
  auto host_it = running_queries_.find(coord_id);
  if (host_it == running_queries_.end()) {
    // In the context of the admission control service, this may happen, eg. if a
    // coordinator is reported as failed by the statestore but a ReleaseQuery rpc from
    // it is delayed in the network and arrives much later.
    LOG(WARNING) << "Unable to find host " << PrintId(coord_id)
                 << " to get resources to release backends for query "
                 << PrintId(query_id) << ", may have already been released.";
    return;
  }
  auto it = host_it->second.find(query_id);
  if (it == host_it->second.end()) {
    // In the context of the admission control service, this may happen, eg. if a
    // ReleaseQueryBackends rpc is delayed in the network and arrives after the
    // ReleaseQuery rpc, so only log as a WARNING.
    LOG(WARNING) << "Unable to find resources to release backends for query "
                 << PrintId(query_id) << ", may have already been released.";
    return;
  }

  RunningQuery& running_query = it->second;
  UpdateStatsOnReleaseForBackends(query_id, running_query, host_addrs);

  // Update num_released_backends_.
  auto released_backends = num_released_backends_.find(query_id);
  if (released_backends != num_released_backends_.end()) {
    released_backends->second -= host_addrs.size();
  } else {
    // In the context of the admission control service, this may happen, eg. if a
    // ReleaseQueryBackends rpc is delayed in the network and arrives after the
    // ReleaseQuery rpc, so only log as a WARNING.
    string err_msg = Substitute(
        "Unable to find num released backends for query $0", PrintId(query_id));
    LOG(WARNING) << err_msg;
  }

  if (VLOG_IS_ON(2)) {
    stringstream ss;
    ss << "Released query backend(s) ";
    for (const auto& host_addr : host_addrs) ss << host_addr << " ";
    ss << "for query id=" << PrintId(query_id) << " "
       << GetPoolStats(running_query.request_pool)->DebugString();
    VLOG(2) << ss.str();
  }
  pending_dequeue_ = true;
}

vector<UniqueIdPB> AdmissionController::CleanupQueriesForHost(
    const UniqueIdPB& coord_id, const std::unordered_set<UniqueIdPB> query_ids) {
  vector<UniqueIdPB> to_clean_up;
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    auto host_it = running_queries_.find(coord_id);
    if (host_it == running_queries_.end()) {
      // This is expected if a coordinator has not submitted any queries yet, eg. at
      // startup, so we log at a higher level to avoid log spam.
      VLOG(3) << "Unable to find host " << PrintId(coord_id)
              << " to cleanup queries for.";
      return to_clean_up;
    }
    for (auto entry : host_it->second) {
      const UniqueIdPB& query_id = entry.first;
      auto it = query_ids.find(query_id);
      if (it == query_ids.end()) {
        to_clean_up.push_back(query_id);
      }
    }
  }

  for (const UniqueIdPB& query_id : to_clean_up) {
    LOG(INFO) << "Releasing resources for query " << PrintId(query_id)
              << " as it's coordinator " << PrintId(coord_id)
              << " reports that it is no longer registered.";
    ReleaseQuery(query_id, coord_id, -1, /* release_remaining_backends */ true);
  }
  return to_clean_up;
}

std::unordered_map<UniqueIdPB, vector<UniqueIdPB>>
AdmissionController::CancelQueriesOnFailedCoordinators(
    std::unordered_set<UniqueIdPB> current_backends) {
  std::unordered_map<UniqueIdPB, vector<UniqueIdPB>> to_clean_up;
  {
    lock_guard<mutex> lock(admission_ctrl_lock_);
    for (const auto& entry : running_queries_) {
      const UniqueIdPB& coord_id = entry.first;
      auto it = current_backends.find(coord_id);
      if (it == current_backends.end()) {
        LOG(INFO) << "Detected that coordinator " << PrintId(coord_id)
                  << " is no longer in the cluster membership. Cancelling "
                  << entry.second.size() << " queries for this coordinator.";
        to_clean_up.insert(make_pair(coord_id, vector<UniqueIdPB>()));
        for (auto entry2 : entry.second) {
          to_clean_up[coord_id].push_back(entry2.first);
        }
      }
    }
  }

  for (const auto& entry : to_clean_up) {
    const UniqueIdPB& coord_id = entry.first;
    for (const UniqueIdPB& query_id : entry.second) {
      ReleaseQuery(query_id, coord_id, -1, /* release_remaining_backends */ true);
    }

    lock_guard<mutex> lock(admission_ctrl_lock_);
    auto it = running_queries_.find(coord_id);
    // It's possible that more queries will have been scheduled for this coordinator
    // since we constructed 'to_clean_up' above, eg. because they were queued. In that
    // case, their resources will be released on the next statestore heartbeat.
    // TODO: handle removing queued queries when their coordinator goes down.
    if (it->second.size() == 0) {
      running_queries_.erase(it);
    }
  }
  return to_clean_up;
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
    AddPoolAndPerHostStatsUpdates(subscriber_topic_updates);

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
    pending_dequeue_ = true;
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
  string topic_key_prefix;
  string topic_key_suffix;
  string pool_name;
  string topic_backend_id;
  for (const TTopicItem& item : topic_updates) {
    if (!ParseTopicKey(item.key, &topic_key_prefix, &topic_key_suffix)) continue;
    if (topic_key_prefix == TOPIC_KEY_POOL_PREFIX) {
      if (!ParsePoolTopicKey(topic_key_suffix, &pool_name, &topic_backend_id)) continue;
      // The topic entry from this subscriber is handled specially; the stats coming
      // from the statestore are likely already outdated.
      if (topic_backend_id == host_id_) continue;
      if (item.deleted) {
        GetPoolStats(pool_name)->UpdateRemoteStats(topic_backend_id, nullptr);
        continue;
      }
      TPoolStats remote_update;
      uint32_t len = item.value.size();
      Status status =
          DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(item.value.data()), &len,
              false, &remote_update);
      if (!status.ok()) {
        VLOG_QUERY << "Error deserializing pool update with key: " << item.key;
        continue;
      }
      GetPoolStats(pool_name)->UpdateRemoteStats(topic_backend_id, &remote_update);
    } else if (topic_key_prefix == TOPIC_KEY_STAT_PREFIX) {
      topic_backend_id = topic_key_suffix;
      if (topic_backend_id == host_id_) continue;
      if (item.deleted) {
        remote_per_host_stats_.erase(topic_backend_id);
        continue;
      }
      TPerHostStatsUpdate remote_update;
      uint32_t len = item.value.size();
      Status status =
          DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(item.value.data()), &len,
              false, &remote_update);
      if (!status.ok()) {
        VLOG_QUERY << "Error deserializing stats update with key: " << item.key;
        continue;
      }
      PerHostStats& stats = remote_per_host_stats_[topic_backend_id];
      for(const auto& elem: remote_update.per_host_stats) {
        stats[elem.host_addr] = elem.stats;
      }
    } else {
      VLOG_QUERY << "Invalid topic key prefix: " << topic_key_prefix;
    }
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

Status AdmissionController::ComputeGroupScheduleStates(
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
  std::vector<GroupScheduleState>* output_schedules = &queue_node->group_states;
  output_schedules->clear();

  // Queries may arrive before we've gotten a statestore update containing the descriptor
  // for their coordinator, in which case we queue the query until it arrives. It's also
  // possible (though very unlikely) that the coordinator was removed from the cluster
  // membership after submitting this query for admission. Currently in this case the
  // query will remain queued until it times out, but we can consider detecting failed
  // coordinators and cleaning up their queued queries.
  auto it = membership_snapshot->current_backends.find(PrintId(request.coord_id));
  if (it == membership_snapshot->current_backends.end()) {
    queue_node->not_admitted_reason = REASON_COORDINATOR_NOT_FOUND;
    LOG(WARNING) << queue_node->not_admitted_reason;
    return Status::OK();
  }
  const BackendDescriptorPB& coord_desc = it->second;

  vector<const ExecutorGroup*> executor_groups =
      GetExecutorGroupsForQuery(membership_snapshot->executor_groups, request);

  if (executor_groups.empty()) {
    queue_node->not_admitted_reason = REASON_NO_EXECUTOR_GROUPS;
    LOG(WARNING) << queue_node->not_admitted_reason;
    return Status::OK();
  }

  // Collect all coordinators if needed for the request.
  ExecutorGroup coords = request.request.include_all_coordinators ?
      membership_snapshot->GetCoordinators() : ExecutorGroup("all-coordinators");

  // We loop over the executor groups in a deterministic order. If
  // --balance_queries_across_executor_groups set to true, executor groups with more
  // available memory and slots will be processed first. If the flag set to false, we will
  // process executor groups in alphanumerically sorted order.
  for (const ExecutorGroup* executor_group : executor_groups) {
    DCHECK(executor_group->IsHealthy()
        || cluster_membership_mgr_->GetEmptyExecutorGroup() == executor_group)
        << executor_group->name();
    // Create a temporary ExecutorGroup if we need to filter out executors with the
    // the set of blacklisted executor addresses in the request.
    // Note: Coordinator-only query should not be failed due to RPC error, nor make
    // executor to be blacklisted.
    const ExecutorGroup* orig_executor_group = executor_group;
    std::unique_ptr<ExecutorGroup> temp_executor_group;
    if (!request.blacklisted_executor_addresses.empty()
        && cluster_membership_mgr_->GetEmptyExecutorGroup() != executor_group) {
      temp_executor_group.reset(ExecutorGroup::GetFilteredExecutorGroup(
          executor_group, request.blacklisted_executor_addresses));
      // If all executors are blacklisted, the retried query cannot be executed so
      // the Scheduler::Schedule() can be skipped.
      if (temp_executor_group.get()->NumExecutors() == 0) continue;
      executor_group = temp_executor_group.get();
    }

    unique_ptr<ScheduleState> group_state = make_unique<ScheduleState>(request.query_id,
        request.request, request.query_options, request.summary_profile, false);
    const string& group_name = executor_group->name();
    VLOG(3) << "Scheduling for executor group: " << group_name << " with "
            << executor_group->NumExecutors() << " executors";
    const Scheduler::ExecutorConfig group_config = {*executor_group, coord_desc, coords};
    RETURN_IF_ERROR(scheduler_->Schedule(group_config, group_state.get()));
    DCHECK(!group_state->executor_group().empty());
    output_schedules->emplace_back(std::move(group_state), *orig_executor_group);
  }
  if (output_schedules->empty()) {
    // Retried query could not be scheduled since all executors are blacklisted.
    // To keep consistent with the other blacklisting logic, set not_admitted_reason as
    // REASON_NO_EXECUTOR_GROUPS.
    queue_node->not_admitted_reason = REASON_NO_EXECUTOR_GROUPS;
    LOG(WARNING) << queue_node->not_admitted_reason;
  }
  return Status::OK();
}

bool AdmissionController::FindGroupToAdmitOrReject(
    ClusterMembershipMgr::SnapshotPtr membership_snapshot, const TPoolConfig& pool_config,
    bool admit_from_queue, PoolStats* pool_stats, QueueNode* queue_node,
    bool& coordinator_resource_limited, bool* is_trivial) {
  // Check for rejection based on current cluster size
  const string& pool_name = pool_stats->name();
  string rejection_reason;
  if (RejectForCluster(pool_name, pool_config, admit_from_queue, &rejection_reason)) {
    DCHECK(!rejection_reason.empty());
    queue_node->not_admitted_reason = rejection_reason;
    return false;
  }

  // Compute schedules
  Status ret = ComputeGroupScheduleStates(membership_snapshot, queue_node);
  if (!ret.ok()) {
    DCHECK(queue_node->not_admitted_reason.empty());
    queue_node->not_admitted_reason = Substitute(REASON_SCHEDULER_ERROR, ret.GetDetail());
    return false;
  }
  if (queue_node->group_states.empty()) {
    DCHECK(!queue_node->not_admitted_reason.empty());
    return true;
  }

  // Get Coordinator Backend for the given admission request
  const AdmissionRequest& request = queue_node->admission_request;
  auto it = membership_snapshot->current_backends.find(PrintId(request.coord_id));
  if (it == membership_snapshot->current_backends.end()) {
    queue_node->not_admitted_reason = REASON_COORDINATOR_NOT_FOUND;
    LOG(WARNING) << queue_node->not_admitted_reason;
    return true;
  }
  const BackendDescriptorPB& coord_desc = it->second;

  for (GroupScheduleState& group_state : queue_node->group_states) {
    const ExecutorGroup& executor_group = group_state.executor_group;
    ScheduleState* state = group_state.state.get();
    state->UpdateMemoryRequirements(pool_config,
        coord_desc.admit_mem_limit(),
        executor_group.GetPerExecutorMemLimitForAdmission());

    const string& group_name = executor_group.name();
    int64_t group_size = executor_group.NumExecutors();
    VLOG(3) << "Trying to admit query to pool " << pool_name << " in executor group "
            << group_name << " (" << group_size << " executors)";

    const int64_t max_queued = GetMaxQueuedForPool(pool_config);
    const int64_t max_mem = GetMaxMemForPool(pool_config);
    const int64_t max_requests = GetMaxRequestsForPool(pool_config);
    VLOG_QUERY << "Trying to admit id=" << PrintId(state->query_id())
               << " in pool_name=" << pool_name << " executor_group_name=" << group_name
               << " per_host_mem_estimate="
               << PrintBytes(state->GetPerExecutorMemoryEstimate())
               << " dedicated_coord_mem_estimate="
               << PrintBytes(state->GetDedicatedCoordMemoryEstimate())
               << " max_requests=" << max_requests << " max_queued=" << max_queued
               << " max_mem=" << PrintBytes(max_mem) << " is_trivial_query="
               << PrettyPrinter::Print(state->GetIsTrivialQuery(), TUnit::NONE);
    VLOG_QUERY << "Stats: " << pool_stats->DebugString();

    if (state->GetIsTrivialQuery() && CanAdmitTrivialRequest(*state)) {
      // The trivial query is supposed to be a subset of the coord-only query,
      // so the executor group should be an empty group.
      DCHECK_EQ(&executor_group, cluster_membership_mgr_->GetEmptyExecutorGroup());
      VLOG_QUERY << "Admitted by trivial query policy.";
      queue_node->admitted_schedule = std::move(group_state.state);
      DCHECK(is_trivial != nullptr);
      if (is_trivial != nullptr) *is_trivial = true;
      return true;
    }

    // Query is rejected if the rejection check fails on *any* group.
    if (RejectForSchedule(*state, pool_config, &rejection_reason)) {
      DCHECK(!rejection_reason.empty());
      queue_node->not_admitted_reason = rejection_reason;
      return false;
    }

    if (CanAdmitRequest(*state, pool_config, admit_from_queue,
            &queue_node->not_admitted_reason, &queue_node->not_admitted_details,
            coordinator_resource_limited)) {
      queue_node->admitted_schedule = std::move(group_state.state);
      return true;
    } else {
      VLOG_RPC << "Cannot admit query " << queue_node->admission_request.query_id
               << " to group " << group_name << ": " << queue_node->not_admitted_reason
               << " Details:" << queue_node->not_admitted_details;
    }
  }
  return true;
}

void AdmissionController::PoolStats::UpdateMemTrackerStats() {
  // May be NULL if no queries have ever executed in this pool on this node but another
  // node sent stats for this pool.
  MemTracker* tracker =
      parent_->pool_mem_trackers_->GetRequestPoolMemTracker(name_, false);

  if (tracker) {
    // Update local_stats_ with the query Ids of the top 5 queries, plus the min, the max,
    // the total memory consumption, and the number of all queries running on this
    // host tracked by this pool.
    tracker->UpdatePoolStatsForQueries(5 /*limit*/, this->local_stats_);
  }

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

void AdmissionController::AddPoolAndPerHostStatsUpdates(
    vector<TTopicDelta>* topic_updates) {
  // local_stats_ are updated eagerly except for backend_mem_reserved (which isn't used
  // for local admission control decisions). Update that now before sending local_stats_.
  for (auto& entry : pool_stats_) {
    entry.second.UpdateMemTrackerStats();
  }
  if (pools_for_updates_.empty()) {
    // No pool updates means no changes to host stats as well, so just return.
    return;
  }
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
      topic_delta.topic_entries.pop_back();
    }
  }
  pools_for_updates_.clear();

  // Now add the host stats
  topic_delta.topic_entries.push_back(TTopicItem());
  TTopicItem& topic_item = topic_delta.topic_entries.back();
  topic_item.key = Substitute("$0$1", TOPIC_KEY_STAT_PREFIX, host_id_);
  TPerHostStatsUpdate update;
  for (const auto& elem : host_stats_) {
    update.per_host_stats.emplace_back();
    TPerHostStatsUpdateElement& inserted_elem = update.per_host_stats.back();
    inserted_elem.__set_host_addr(elem.first);
    inserted_elem.__set_stats(elem.second);
  }
  Status status =
      thrift_serializer_.SerializeToString(&update, &topic_item.value);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to serialize host stats: " << status.GetDetail();
    topic_delta.topic_entries.pop_back();
  }
}

void AdmissionController::DequeueLoop() {
  unique_lock<mutex> lock(admission_ctrl_lock_);
  while (true) {
    if (done_) break;
    while (!pending_dequeue_) {
      dequeue_cv_.Wait(lock);
    }
    pending_dequeue_ = false;
    ClusterMembershipMgr::SnapshotPtr membership_snapshot =
        cluster_membership_mgr_->GetSnapshot();

    // If a query was queued while the cluster is still starting up but the client facing
    // services have already started to accept connections, the whole membership can still
    // be empty.
    if (membership_snapshot->executor_groups.empty()) continue;

    for (const PoolConfigMap::value_type& entry: pool_config_map_) {
      const string& pool_name = entry.first;
      const TPoolConfig& pool_config = entry.second;
      PoolStats* stats = GetPoolStats(pool_name, /* dcheck_exists=*/true);

      if (stats->local_stats().num_queued == 0) continue; // Nothing to dequeue
      DCHECK_GE(stats->agg_num_queued(), stats->local_stats().num_queued);

      RequestQueue& queue = request_queue_map_[pool_name];
      int64_t max_to_dequeue = GetMaxToDequeue(queue, stats, pool_config);
      VLOG_RPC << "Dequeue thread will try to admit " << max_to_dequeue << " requests"
               << ", pool=" << pool_name
               << ", num_queued=" << stats->local_stats().num_queued
               << " cluster_size=" << GetClusterSize(*membership_snapshot);
      if (max_to_dequeue == 0) continue; // to next pool.

      while (max_to_dequeue > 0 && !queue.empty()) {
        QueueNode* queue_node = queue.head();
        DCHECK(queue_node != nullptr);
        // Find a group that can admit the query
        bool is_cancelled = queue_node->admit_outcome->IsSet()
            && queue_node->admit_outcome->Get() == AdmissionOutcome::CANCELLED;

        bool coordinator_resource_limited = false;
        bool is_trivial = false;
        bool is_rejected = !is_cancelled
            && !FindGroupToAdmitOrReject(membership_snapshot, pool_config,
                   /* admit_from_queue=*/true, stats, queue_node,
                   coordinator_resource_limited, &is_trivial);

        if (!is_cancelled && !is_rejected
            && queue_node->admitted_schedule.get() == nullptr) {
          // If no group was found, stop trying to dequeue.
          // TODO(IMPALA-2968): Requests further in the queue may be blocked
          // unnecessarily. Consider a better policy once we have better test scenarios.
          LogDequeueFailed(queue_node, queue_node->not_admitted_reason);
          if (coordinator_resource_limited) {
            // Dequeue failed because of a resource issue that can't be solved by adding
            // more executor groups. The common reason for this is that we are hitting a
            // limit on the coordinator.
            total_dequeue_failed_coordinator_limited_->Increment(1);
          }
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

        const UniqueIdPB& query_id = queue_node->admission_request.query_id;
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
        AdmitQuery(queue_node, true /* was_queued */, is_trivial);
      }
      pools_for_updates_.insert(pool_name);
    }
  }
}

int64_t AdmissionController::GetQueueTimeoutForPoolMs(const TPoolConfig& pool_config) {
  int64_t queue_wait_timeout_ms = pool_config.__isset.queue_timeout_ms ?
      pool_config.queue_timeout_ms :
      FLAGS_queue_wait_timeout_ms;
  return max<int64_t>(0, queue_wait_timeout_ms);
}

int64_t AdmissionController::GetMaxToDequeue(
    RequestQueue& queue, PoolStats* stats, const TPoolConfig& pool_config) {
  if (PoolLimitsRunningQueriesCount(pool_config)) {
    const int64_t max_requests = GetMaxRequestsForPool(pool_config);
    const int64_t total_available = max_requests - stats->agg_num_running();
    if (total_available <= 0) {
      // There is a limit for the number of running queries, so we can
      // see that nothing can run in this pool.
      // This can happen in the case of over-admission.
      if (!queue.empty()) {
        LogDequeueFailed(queue.head(),
            Substitute(QUEUED_NUM_RUNNING, stats->agg_num_running(), max_requests,
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
    const ScheduleState& state) {
  DCHECK(!state.request_pool().empty());
  return GetPoolStats(state.request_pool());
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

void AdmissionController::AdmitQuery(QueueNode* node, bool was_queued, bool is_trivial) {
  ScheduleState* state = node->admitted_schedule.get();
  VLOG_RPC << "For Query " << PrintId(state->query_id())
           << " per_backend_mem_limit set to: "
           << PrintBytes(state->per_backend_mem_limit())
           << " per_backend_mem_to_admit set to: "
           << PrintBytes(state->per_backend_mem_to_admit())
           << " coord_backend_mem_limit set to: "
           << PrintBytes(state->coord_backend_mem_limit())
           << " coord_backend_mem_to_admit set to: "
           << PrintBytes(state->coord_backend_mem_to_admit());
  // Update memory and number of queries.
  UpdateStatsOnAdmission(*state, is_trivial);
  UpdateExecGroupMetric(state->executor_group(), 1);
  // Update summary profile.
  const string& admission_result = was_queued ?
      PROFILE_INFO_VAL_ADMIT_QUEUED :
      (is_trivial ? PROFILE_INFO_VAL_ADMIT_TRIVIAL : PROFILE_INFO_VAL_ADMIT_IMMEDIATELY);
  state->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMISSION_RESULT, admission_result);
  state->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_ADMITTED_MEM, PrintBytes(state->GetClusterMemoryToAdmit()));
  state->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_EXECUTOR_GROUP, state->executor_group());
  state->summary_profile()->AddInfoString(PROFILE_INFO_KEY_EXECUTOR_GROUP_QUERY_LOAD,
      std::to_string(GetExecGroupQueryLoad(state->executor_group())));
  // We may have admitted based on stale information. Include a warning in the profile
  // if this this may be the case.
  int64_t time_since_update_ms;
  string staleness_detail = GetStalenessDetailLocked("", &time_since_update_ms);
  // IMPALA-8235: convert to TIME_NS because of issues with tools consuming TIME_MS.
  COUNTER_SET(ADD_COUNTER(state->summary_profile(),
                  PROFILE_TIME_SINCE_LAST_UPDATE_COUNTER_NAME, TUnit::TIME_NS),
      static_cast<int64_t>(time_since_update_ms * NANOS_PER_MICRO * MICROS_PER_MILLI));
  if (!staleness_detail.empty()) {
    state->summary_profile()->AddInfoString(
        PROFILE_INFO_KEY_STALENESS_WARNING, staleness_detail);
  }
  DCHECK(num_released_backends_.find(state->query_id()) == num_released_backends_.end());
  num_released_backends_[state->query_id()] = state->per_backend_schedule_states().size();

  // Store info about the admitted resources so that we can release them.
  auto it = running_queries_.find(node->admission_request.coord_id);
  if (it == running_queries_.end()) {
    auto insert_result =
        running_queries_.insert(make_pair(node->admission_request.coord_id,
            std::unordered_map<UniqueIdPB, RunningQuery>()));
    DCHECK(insert_result.second);
    it = insert_result.first;
  }
  DCHECK(it->second.find(state->query_id()) == it->second.end());
  RunningQuery& running_query = it->second[state->query_id()];
  running_query.request_pool = state->request_pool();
  running_query.executor_group = state->executor_group();
  running_query.is_trivial = is_trivial;
  for (const auto& entry : state->per_backend_schedule_states()) {
    BackendAllocation& allocation = running_query.per_backend_resources[entry.first];
    allocation.slots_to_use = entry.second.exec_params->slots_to_use();
    allocation.mem_to_admit = GetMemToAdmit(*state, entry.second);
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
    if (node->group_states.empty()) {
      Value query_info(kObjectType);
      query_info.AddMember("query_id", "N/A", document->GetAllocator());
      query_info.AddMember("mem_limit", 0, document->GetAllocator());
      query_info.AddMember("mem_limit_to_admit", 0, document->GetAllocator());
      query_info.AddMember("num_backends", 0, document->GetAllocator());
      queued_queries.PushBack(query_info, document->GetAllocator());
      return true;
    }
    ScheduleState* state = node->group_states.begin()->state.get();
    Value query_info(kObjectType);
    Value query_id(PrintId(state->query_id()).c_str(), document->GetAllocator());
    query_info.AddMember("query_id", query_id, document->GetAllocator());
    query_info.AddMember(
        "mem_limit", state->per_backend_mem_limit(), document->GetAllocator());
    query_info.AddMember("mem_limit_to_admit", state->per_backend_mem_to_admit(),
        document->GetAllocator());
    query_info.AddMember(
        "coord_mem_limit", state->coord_backend_mem_limit(), document->GetAllocator());
    query_info.AddMember("coord_mem_to_admit", state->coord_backend_mem_to_admit(),
        document->GetAllocator());
    query_info.AddMember("num_backends", state->per_backend_schedule_states().size(),
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
  pool->AddMember("pool_queue_timeout", metrics_.pool_queue_timeout->GetValue(),
      document->GetAllocator());
  pool->AddMember("max_query_mem_limit", metrics_.max_query_mem_limit->GetValue(),
      document->GetAllocator());
  pool->AddMember("min_query_mem_limit", metrics_.min_query_mem_limit->GetValue(),
      document->GetAllocator());
  pool->AddMember("clamp_mem_limit_query_option",
      metrics_.clamp_mem_limit_query_option->GetValue(), document->GetAllocator());
  pool->AddMember("max_query_cpu_core_per_node_limit",
      metrics_.max_query_cpu_core_per_node_limit->GetValue(), document->GetAllocator());
  pool->AddMember("max_query_cpu_core_coordinator_limit",
      metrics_.max_query_cpu_core_coordinator_limit->GetValue(),
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
  metrics_.pool_queue_timeout = parent_->metrics_group_->AddGauge(
      POOL_QUEUE_TIMEOUT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_query_mem_limit = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.min_query_mem_limit = parent_->metrics_group_->AddGauge(
      POOL_MIN_QUERY_MEM_LIMIT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.clamp_mem_limit_query_option = parent_->metrics_group_->AddProperty<bool>(
      POOL_CLAMP_MEM_LIMIT_QUERY_OPTION_METRIC_KEY_FORMAT, false, name_);
  metrics_.max_query_cpu_core_per_node_limit = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUERY_CPU_CORE_PER_NODE_LIMIT_METRIC_KEY_FORMAT, 0, name_);
  metrics_.max_query_cpu_core_coordinator_limit = parent_->metrics_group_->AddGauge(
      POOL_MAX_QUERY_CPU_CORE_COORDINATOR_LIMIT_METRIC_KEY_FORMAT, 0, name_);
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
  return Substitute(
      "$0$1$2$3", TOPIC_KEY_POOL_PREFIX, pool_name, TOPIC_KEY_DELIMITER, backend_id);
}

const pair<int64_t, int64_t> AdmissionController::GetAvailableMemAndSlots(
    const ExecutorGroup& group) const {
  int64_t total_mem_limit = 0;
  int64_t total_slots = 0;
  int64_t agg_effective_mem_reserved = 0;
  int64_t agg_slots_in_use = 0;
  for (const BackendDescriptorPB& be_desc : group.GetAllExecutorDescriptors()) {
    total_mem_limit += be_desc.admit_mem_limit();
    total_slots += be_desc.admission_slots();
    int64_t host_mem_reserved = 0;
    int64_t host_mem_admit = 0;
    const string& host = NetworkAddressPBToString(be_desc.address());
    auto stats = host_stats_.find(host);
    if (stats != host_stats_.end()) {
      host_mem_reserved = stats->second.mem_reserved;
      host_mem_admit += stats->second.mem_admitted;
      agg_slots_in_use += stats->second.slots_in_use;
    }
    for (const auto& remote_entry : remote_per_host_stats_) {
      auto remote_stats = remote_entry.second.find(host);
      if (remote_stats != remote_entry.second.end()) {
        host_mem_admit += remote_stats->second.mem_admitted;
        agg_slots_in_use += remote_stats->second.slots_in_use;
      }
    }
    agg_effective_mem_reserved += std::max(host_mem_reserved, host_mem_admit);
  }
  DCHECK_GE(total_mem_limit, agg_effective_mem_reserved);
  DCHECK_GE(total_slots, agg_slots_in_use);
  return make_pair(
      total_mem_limit - agg_effective_mem_reserved, total_slots - agg_slots_in_use);
}

vector<const ExecutorGroup*> AdmissionController::GetExecutorGroupsForQuery(
    const ClusterMembershipMgr::ExecutorGroups& all_groups,
    const AdmissionRequest& request) {
  vector<const ExecutorGroup*> matching_groups;
  if (scheduler_->IsCoordinatorOnlyQuery(request.request)) {
    // Coordinator only queries can run regardless of the presence of exec groups. This
    // empty group works as a proxy to schedule coordinator only queries.
    matching_groups.push_back(cluster_membership_mgr_->GetEmptyExecutorGroup());
    return matching_groups;
  }
  const string& pool_name = request.request.query_ctx.request_pool;
  string prefix(pool_name + POOL_GROUP_DELIMITER);
  // We search for matching groups before the health check so that we don't fall back to
  // the default group in case there are matching but unhealthy groups.
  for (const auto& it : all_groups) {
    StringPiece name(it.first);
    if (name.starts_with(prefix)) matching_groups.push_back(&it.second);
  }
  if (matching_groups.empty()) {
    auto default_it = all_groups.find(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
    if (default_it == all_groups.end()) return matching_groups;
    VLOG(3) << "Checking default executor group for pool " << pool_name;
    matching_groups.push_back(&default_it->second);
  }
  // Filter out unhealthy groups.
  auto erase_from = std::remove_if(matching_groups.begin(), matching_groups.end(),
      [](const ExecutorGroup* g) { return !g->IsHealthy(); });
  matching_groups.erase(erase_from, matching_groups.end());
  // Sort executor groups by name.
  auto cmp = [](const ExecutorGroup* a, const ExecutorGroup* b) {
    return a->name() < b->name();
  };
  sort(matching_groups.begin(), matching_groups.end(), cmp);
  if (FLAGS_balance_queries_across_executor_groups) {
    // Sort executor groups by available memory and slots in descending order, we
    // prioritize executor groups that with more available memory, when their available
    // memory are same we choose the one with more available slots, when their available
    // memory and slots are same we choose on an alphabetical basis.
    auto available_resource_cmp = [this](const ExecutorGroup* a, const ExecutorGroup* b) {
      return GetAvailableMemAndSlots(*a) > GetAvailableMemAndSlots(*b);
    };
    sort(matching_groups.begin(), matching_groups.end(), available_resource_cmp);
  }
  return matching_groups;
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

int64_t AdmissionController::GetMaxMemForPool(const TPoolConfig& pool_config) {
  return pool_config.max_mem_resources;
}

int64_t AdmissionController::GetMaxRequestsForPool(const TPoolConfig& pool_config) {
  return pool_config.max_requests;
}

int64_t AdmissionController::GetMaxQueuedForPool(const TPoolConfig& pool_config) {
  return pool_config.max_queued;
}

bool AdmissionController::PoolDisabled(const TPoolConfig& pool_config) {
  return (pool_config.max_requests == 0 || pool_config.max_mem_resources == 0);
}

bool AdmissionController::PoolLimitsRunningQueriesCount(const TPoolConfig& pool_config) {
  return pool_config.max_requests > 0;
}

int64_t AdmissionController::GetMemToAdmit(
    const ScheduleState& state, const BackendScheduleState& backend_schedule_state) {
  return backend_schedule_state.exec_params->is_coord_backend() ?
      state.coord_backend_mem_to_admit() :
      state.per_backend_mem_to_admit();
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
    for (const BackendDescriptorPB& be_desc : group.GetAllExecutorDescriptors()) {
      const string& host = NetworkAddressPBToString(be_desc.address());
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
  if (entry != exec_group_query_load_map_.end()) {
    DCHECK_GE(entry->second->GetValue() + delta, 0);
    entry->second->Increment(delta);
  }
}

int64_t AdmissionController::GetExecGroupQueryLoad(const string& grp_name) {
  auto entry = exec_group_query_load_map_.find(grp_name);
  if (entry != exec_group_query_load_map_.end()) {
    return entry->second->GetValue();
  }
  return 0;
}

} // namespace impala
