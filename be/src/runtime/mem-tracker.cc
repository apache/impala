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

#include "runtime/mem-tracker.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "runtime/bufferpool/reservation-tracker-counters.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/test-info.h"
#include "util/uid-util.h"
#include "util/container-util.h"

#include "common/names.h"

using boost::algorithm::join;
using std::priority_queue;
using std::greater;
using namespace strings;

DEFINE_double_hidden(soft_mem_limit_frac, 0.9, "(Advanced) Soft memory limit as a "
    "fraction of hard memory limit.");

namespace impala {

const string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

// Name for request pool MemTrackers. '$0' is replaced with the pool name.
const string REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT = "RequestPool=$0";

/// Calculate the soft limit for a MemTracker based on the hard limit 'limit'.
static int64_t CalcSoftLimit(int64_t limit) {
  if (limit < 0) return -1;
  double frac = max(0.0, min(1.0, FLAGS_soft_mem_limit_frac));
  return static_cast<int64_t>(limit * frac);
}

MemTracker::MemTracker(int64_t byte_limit, const string& label, MemTracker* parent,
    bool log_usage_if_zero, bool is_query_mem_tracker, const TUniqueId& query_id)
  : is_query_mem_tracker_(is_query_mem_tracker),
    query_id_(query_id),
    limit_(byte_limit),
    soft_limit_(CalcSoftLimit(byte_limit)),
    label_(label),
    parent_(parent),
    consumption_(&local_counter_),
    local_counter_(TUnit::BYTES),
    consumption_metric_(NULL),
    log_usage_if_zero_(log_usage_if_zero),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL),
    limit_metric_(NULL) {
  Init();
}

MemTracker::MemTracker(RuntimeProfile* profile, int64_t byte_limit,
    const std::string& label, MemTracker* parent)
  : limit_(byte_limit),
    soft_limit_(CalcSoftLimit(byte_limit)),
    label_(label),
    parent_(parent),
    consumption_(profile->AddHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES)),
    local_counter_(TUnit::BYTES),
    consumption_metric_(NULL),
    log_usage_if_zero_(true),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL),
    limit_metric_(NULL) {
  Init();
}

MemTracker::MemTracker(IntGauge* consumption_metric,
    int64_t byte_limit, const string& label, MemTracker* parent)
  : limit_(byte_limit),
    soft_limit_(CalcSoftLimit(byte_limit)),
    label_(label),
    parent_(parent),
    consumption_(&local_counter_),
    local_counter_(TUnit::BYTES),
    consumption_metric_(consumption_metric),
    log_usage_if_zero_(true),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL),
    limit_metric_(NULL) {
  Init();
}

void MemTracker::Init() {
  DCHECK_GE(limit_, -1);
  DCHECK_LE(soft_limit_, limit_);
  if (parent_ != NULL) parent_->AddChildTracker(this);
  // populate all_trackers_ and limit_trackers_
  MemTracker* tracker = this;
  while (tracker != NULL) {
    all_trackers_.push_back(tracker);
    if (tracker->has_limit()) limit_trackers_.push_back(tracker);
    tracker = tracker->parent_;
  }
  DCHECK_GT(all_trackers_.size(), 0);
  DCHECK_EQ(all_trackers_[0], this);
}

void MemTracker::AddChildTracker(MemTracker* tracker) {
  lock_guard<SpinLock> l(child_trackers_lock_);
  tracker->child_tracker_it_ = child_trackers_.insert(child_trackers_.end(), tracker);
}

void MemTracker::Close() {
  if (closed_) return;
  if (consumption_metric_ == nullptr) {
    DCHECK_EQ(consumption_->current_value(), 0) << label_ << "\n"
                                                << GetStackTrace() << "\n"
                                                << LogUsage(UNLIMITED_DEPTH);
  }
  closed_ = true;
}

void MemTracker::CloseAndUnregisterFromParent() {
  Close();
  lock_guard<SpinLock> l(parent_->child_trackers_lock_);
  parent_->child_trackers_.erase(child_tracker_it_);
  child_tracker_it_ = parent_->child_trackers_.end();
}

void MemTracker::EnableReservationReporting(const ReservationTrackerCounters& counters) {
  delete reservation_counters_.Swap(new ReservationTrackerCounters(counters));
}

int64_t MemTracker::GetLowestLimit(MemLimit mode) const {
  if (limit_trackers_.empty()) return -1;
  int64_t min_limit = numeric_limits<int64_t>::max();
  for (MemTracker* limit_tracker : limit_trackers_) {
    DCHECK(limit_tracker->has_limit());
    min_limit = min(min_limit, limit_tracker->GetLimit(mode));
  }
  return min_limit;
}

int64_t MemTracker::SpareCapacity(MemLimit mode) const {
  int64_t result = numeric_limits<int64_t>::max();
  for (MemTracker* tracker : limit_trackers_) {
    int64_t mem_left = tracker->GetLimit(mode) - tracker->consumption();
    result = std::min(result, mem_left);
  }
  return result;
}

void MemTracker::RefreshConsumptionFromMetric() {
  DCHECK(consumption_metric_ != nullptr);
  consumption_->Set(consumption_metric_->GetValue());
}

int64_t MemTracker::GetPoolMemReserved() {
  // Pool trackers should have a pool_name_ and no limit.
  DCHECK(!pool_name_.empty());
  DCHECK_EQ(limit_, -1) << LogUsage(UNLIMITED_DEPTH);

  int64_t mem_reserved = 0L;
  lock_guard<SpinLock> l(child_trackers_lock_);
  for (MemTracker* child : child_trackers_) {
    int64_t child_limit = child->limit();
    bool query_exec_finished = child->query_exec_finished_.Load() != 0;
    if (child_limit > 0 && !query_exec_finished) {
      // Make sure we don't overflow if the query limits are set to ridiculous values.
      mem_reserved += std::min(child_limit, MemInfo::physical_mem());
    } else {
      DCHECK(query_exec_finished || child_limit == -1)
          << child->LogUsage(UNLIMITED_DEPTH);
      mem_reserved += child->consumption();
    }
  }
  return mem_reserved;
}

MemTracker* PoolMemTrackerRegistry::GetRequestPoolMemTracker(
    const string& pool_name, bool create_if_not_present) {
  DCHECK(!pool_name.empty());
  lock_guard<SpinLock> l(pool_to_mem_trackers_lock_);
  PoolTrackersMap::iterator it = pool_to_mem_trackers_.find(pool_name);
  if (it != pool_to_mem_trackers_.end()) {
    MemTracker* tracker = it->second.get();
    DCHECK(pool_name == tracker->pool_name_);
    return tracker;
  }
  if (!create_if_not_present) return nullptr;
  // First time this pool_name registered, make a new object.
  MemTracker* tracker =
      new MemTracker(-1, Substitute(REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT, pool_name),
          ExecEnv::GetInstance()->process_mem_tracker());
  tracker->pool_name_ = pool_name;
  pool_to_mem_trackers_.emplace(pool_name, unique_ptr<MemTracker>(tracker));
  return tracker;
}

MemTracker* MemTracker::CreateQueryMemTracker(const TUniqueId& id,
    int64_t mem_limit, const string& pool_name, ObjectPool* obj_pool) {
  if (mem_limit != -1) {
    if (mem_limit > MemInfo::physical_mem()) {
      LOG(WARNING) << "Memory limit " << PrettyPrinter::Print(mem_limit, TUnit::BYTES)
                   << " exceeds physical memory of "
                   << PrettyPrinter::Print(MemInfo::physical_mem(), TUnit::BYTES);
    }
    VLOG(2) << "Using query memory limit: "
            << PrettyPrinter::Print(mem_limit, TUnit::BYTES);
  }

  MemTracker* pool_tracker =
      ExecEnv::GetInstance()->pool_mem_trackers()->GetRequestPoolMemTracker(
          pool_name, true);
  MemTracker* tracker = obj_pool->Add(new MemTracker(
      mem_limit, Substitute("Query($0)", PrintId(id)), pool_tracker, true, true, id));
  return tracker;
}

MemTracker::~MemTracker() {
  // We should explicitly close MemTrackers in the context of a daemon process. It is ok
  // if backend tests don't call Close() to make tests more concise.
  if (TestInfo::is_test()) Close();
  DCHECK(closed_) << label_;
  delete reservation_counters_.Load();
}

void MemTracker::RegisterMetrics(MetricGroup* metrics, const string& prefix) {
  num_gcs_metric_ = metrics->AddCounter(Substitute("$0.num-gcs", prefix), 0);

  // TODO: Consider a total amount of bytes freed counter
  bytes_freed_by_last_gc_metric_ = metrics->AddGauge(
      Substitute("$0.bytes-freed-by-last-gc", prefix), -1);

  bytes_over_limit_metric_ = metrics->AddGauge(
      Substitute("$0.bytes-over-limit", prefix), -1);

  limit_metric_ = metrics->AddGauge(Substitute("$0.limit", prefix), limit_);
}

void MemTracker::TransferTo(MemTracker* dst, int64_t bytes) {
  DCHECK_EQ(all_trackers_.back(), dst->all_trackers_.back())
      << "Must have same root";
  // Find the common ancestor and update trackers between 'this'/'dst' and
  // the common ancestor. This logic handles all cases, including the
  // two trackers being the same or being ancestors of each other because
  // 'all_trackers_' includes the current tracker.
  int ancestor_idx = all_trackers_.size() - 1;
  int dst_ancestor_idx = dst->all_trackers_.size() - 1;
  while (ancestor_idx > 0 && dst_ancestor_idx > 0
      && all_trackers_[ancestor_idx - 1] == dst->all_trackers_[dst_ancestor_idx - 1]) {
    --ancestor_idx;
    --dst_ancestor_idx;
  }
  MemTracker* common_ancestor = all_trackers_[ancestor_idx];
  ReleaseLocal(bytes, common_ancestor);
  dst->ConsumeLocal(bytes, common_ancestor);
}

// Calling this on the query tracker results in output like:
//
//  Query(4a4c81fedaed337d:4acadfda00000000) Limit=10.00 GB Total=508.28 MB Peak=508.45 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000000: Total=8.00 KB Peak=8.00 KB
//      EXCHANGE_NODE (id=4): Total=0 Peak=0
//      DataStreamRecvr: Total=0 Peak=0
//    Block Manager: Limit=6.68 GB Total=394.00 MB Peak=394.00 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000006: Total=233.72 MB Peak=242.24 MB
//      AGGREGATION_NODE (id=1): Total=139.21 MB Peak=139.84 MB
//      HDFS_SCAN_NODE (id=0): Total=93.94 MB Peak=102.24 MB
//      DataStreamSender (dst_id=2): Total=45.99 KB Peak=85.99 KB
//    Fragment 4a4c81fedaed337d:4acadfda00000003: Total=274.55 MB Peak=274.62 MB
//      AGGREGATION_NODE (id=3): Total=274.50 MB Peak=274.50 MB
//      EXCHANGE_NODE (id=2): Total=0 Peak=0
//      DataStreamRecvr: Total=45.91 KB Peak=684.07 KB
//      DataStreamSender (dst_id=4): Total=680.00 B Peak=680.00 B
//
// If 'reservation_metrics_' are set, we ge a more granular breakdown:
//   TrackerName: Limit=5.00 MB Reservation=5.00 MB OtherMemory=1.04 MB
//                Total=6.04 MB Peak=6.45 MB
//
string MemTracker::LogUsage(int max_recursive_depth, const string& prefix,
    int64_t* logged_consumption) {
  // Make sure the consumption is up to date.
  if (consumption_metric_ != nullptr) RefreshConsumptionFromMetric();
  int64_t curr_consumption = consumption();
  int64_t peak_consumption = consumption_->value();
  if (logged_consumption != nullptr) *logged_consumption = curr_consumption;

  if (!log_usage_if_zero_ && curr_consumption == 0) return "";

  stringstream ss;
  ss << prefix << label_ << ":";
  if (CheckLimitExceeded(MemLimit::HARD)) ss << " memory limit exceeded.";
  if (limit_ > 0) ss << " Limit=" << PrettyPrinter::Print(limit_, TUnit::BYTES);

  ReservationTrackerCounters* reservation_counters = reservation_counters_.Load();
  if (reservation_counters != nullptr) {
    int64_t reservation = reservation_counters->peak_reservation->current_value();
    ss << " Reservation=" << PrettyPrinter::Print(reservation, TUnit::BYTES);
    if (reservation_counters->reservation_limit != nullptr) {
      int64_t limit = reservation_counters->reservation_limit->value();
      ss << " ReservationLimit=" << PrettyPrinter::Print(limit, TUnit::BYTES);
    }
    ss << " OtherMemory="
       << PrettyPrinter::Print(curr_consumption - reservation, TUnit::BYTES);
  }
  ss << " Total=" << PrettyPrinter::Print(curr_consumption, TUnit::BYTES);
  // Peak consumption is not accurate if the metric is lazily updated (i.e.
  // this is a non-root tracker that exists only for reporting purposes).
  // Only report peak consumption if we actually call Consume()/Release() on
  // this tracker or an descendent.
  if (consumption_metric_ == nullptr || parent_ == nullptr) {
    ss << " Peak=" << PrettyPrinter::Print(peak_consumption, TUnit::BYTES);
  }

  // This call does not need the children, so return early.
  if (max_recursive_depth == 0) return ss.str();

  // Recurse and get information about the children
  string new_prefix = Substitute("  $0", prefix);
  int64_t child_consumption;
  string child_trackers_usage;
  {
    lock_guard<SpinLock> l(child_trackers_lock_);
    child_trackers_usage = LogUsage(max_recursive_depth - 1, new_prefix,
        child_trackers_, &child_consumption);
  }
  if (!child_trackers_usage.empty()) ss << "\n" << child_trackers_usage;

  if (parent_ == nullptr) {
    // Log the difference between the metric value and children as "untracked" memory so
    // that the values always add up. This value is not always completely accurate because
    // we did not necessarily get a consistent snapshot of the consumption values for all
    // children at a single moment in time, but is good enough for our purposes.
    int64_t untracked_bytes = curr_consumption - child_consumption;
    ss << "\n"
       << new_prefix << "Untracked Memory: Total="
       << PrettyPrinter::Print(untracked_bytes, TUnit::BYTES);
  }
  return ss.str();
}

string MemTracker::LogUsage(int max_recursive_depth, const string& prefix,
    const list<MemTracker*>& trackers, int64_t* logged_consumption) {
  *logged_consumption = 0;
  vector<string> usage_strings;
  for (MemTracker* tracker : trackers) {
    int64_t tracker_consumption;
    string usage_string = tracker->LogUsage(max_recursive_depth, prefix,
        &tracker_consumption);
    if (!usage_string.empty()) usage_strings.push_back(usage_string);
    *logged_consumption += tracker_consumption;
  }
  return join(usage_strings, "\n");
}

string MemTracker::LogTopNQueries(int limit) {
  if (limit == 0) return "";
  if (this->is_query_mem_tracker_) return LogUsage(0);
  priority_queue<pair<int64_t, string>, vector<pair<int64_t, string>>,
      std::greater<pair<int64_t, string>>>
      min_pq;
  GetTopNQueries(min_pq, limit);
  vector<string> usage_strings(min_pq.size());
  while (!min_pq.empty()) {
    usage_strings.push_back(min_pq.top().second);
    min_pq.pop();
  }
  std::reverse(usage_strings.begin(), usage_strings.end());
  return join(usage_strings, "\n");
}

void MemTracker::GetTopNQueries(
    priority_queue<pair<int64_t, string>, vector<pair<int64_t, string>>,
        greater<pair<int64_t, string>>>& min_pq,
    int limit) {
  lock_guard<SpinLock> l(child_trackers_lock_);
  for (MemTracker* tracker : child_trackers_) {
    if (!tracker->is_query_mem_tracker_) {
      tracker->GetTopNQueries(min_pq, limit);
    } else {
      min_pq.push(pair<int64_t, string>(tracker->consumption(), tracker->LogUsage(0)));
      if (min_pq.size() > limit) min_pq.pop();
    }
  }
}

// Update the memory consumption related fields in pool_stats.
void MemTracker::UpdatePoolStatsForMemoryConsumed(
    int64_t mem_consumed, TPoolStats& pool_stats) {
  if (pool_stats.min_memory_consumed > mem_consumed) {
    pool_stats.min_memory_consumed = mem_consumed;
  }
  if (pool_stats.max_memory_consumed < mem_consumed) {
    pool_stats.max_memory_consumed = mem_consumed;
  }
  pool_stats.total_memory_consumed += mem_consumed;
  pool_stats.num_running++;
}

// Update pool_stats for all queries tracked through query memory trackers:
void MemTracker::UpdatePoolStatsForQueries(int limit, TPoolStats& pool_stats) {
  if (limit == 0) return;
  // Init all memory consumption related fields
  pool_stats.heavy_memory_queries.clear();
  pool_stats.min_memory_consumed = std::numeric_limits<int64_t>::max();
  pool_stats.max_memory_consumed = 0;
  pool_stats.total_memory_consumed = 0;
  pool_stats.num_running = 0;
  // Collect the top 'limit' queries into 'min_pq'.
  MinPriorityQueue min_pq;
  GetTopNQueriesAndUpdatePoolStats(min_pq, limit, pool_stats);
  // Grab all remaining entries from the priority queue and assign them in the descending
  // order of memory consumption to the pool_stats.heavy_memory_queries field in
  // pool_stats.
  auto& queries = pool_stats.heavy_memory_queries;
  queries.clear();
  while (!min_pq.empty()) {
    queries.push_back(min_pq.top());
    min_pq.pop();
  }
  std::reverse(queries.begin(), queries.end());
  // If not a single query is found, set the min_memory_consumed to 0.
  if (pool_stats.num_running == 0) {
    pool_stats.min_memory_consumed = 0;
  }
}

void MemTracker::GetTopNQueriesAndUpdatePoolStats(
    MinPriorityQueue& min_pq, int limit, TPoolStats& pool_stats) {
  lock_guard<SpinLock> l(child_trackers_lock_);
  for (MemTracker* tracker : child_trackers_) {
    if (!tracker->is_query_mem_tracker_) {
      tracker->GetTopNQueriesAndUpdatePoolStats(min_pq, limit, pool_stats);
    } else {
      DCHECK(tracker->is_query_mem_tracker_) << label_;
      int64_t mem_consumed = tracker->consumption();

      THeavyMemoryQuery heavy_memory_query;
      heavy_memory_query.__set_memory_consumed(mem_consumed);
      heavy_memory_query.__set_queryId(tracker->query_id_);

      min_pq.push(heavy_memory_query);
      if (min_pq.size() > limit) min_pq.pop();

      UpdatePoolStatsForMemoryConsumed(mem_consumed, pool_stats);
    }
  }
}

MemTracker* MemTracker::GetQueryMemTracker() {
  MemTracker* tracker = this;
  while (tracker != nullptr && !tracker->is_query_mem_tracker_) {
    tracker = tracker->parent_;
  }
  return tracker;
}

MemTracker* MemTracker::GetRootMemTracker() {
  MemTracker* ancestor = this;
  while (ancestor && ancestor->parent()) {
    ancestor = ancestor->parent();
  }
  return ancestor;
}

Status MemTracker::MemLimitExceeded(MemTracker* mtracker, RuntimeState* state,
    const std::string& details, int64_t failed_allocation_size) {
  DCHECK_GE(failed_allocation_size, 0);
  stringstream ss;
  if (details.size() != 0) ss << details << endl;
  if (failed_allocation_size != 0) {
    if (mtracker != nullptr) ss << mtracker->label();
    ss << " could not allocate "
       << PrettyPrinter::Print(failed_allocation_size, TUnit::BYTES)
       << " without exceeding limit." << endl;
  }
  ss << "Error occurred on backend " << GetBackendString();
  if (state != nullptr) ss << " by fragment " << PrintId(state->fragment_instance_id());
  ss << endl;
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_tracker = exec_env->process_mem_tracker();
  const int64_t process_capacity = process_tracker->SpareCapacity(MemLimit::HARD);
  ss << "Memory left in process limit: "
     << PrettyPrinter::Print(process_capacity, TUnit::BYTES) << endl;

  // Always log the query tracker (if available).
  MemTracker* query_tracker = nullptr;
  if (mtracker != nullptr) {
    query_tracker = mtracker->GetQueryMemTracker();
    if (query_tracker != nullptr) {
      if (query_tracker->has_limit()) {
        const int64_t query_capacity =
            query_tracker->limit() - query_tracker->consumption();
        ss << "Memory left in query limit: "
           << PrettyPrinter::Print(query_capacity, TUnit::BYTES) << endl;
      }
      ss << query_tracker->LogUsage(UNLIMITED_DEPTH);
    }
  }

  // Log the process level if the process tracker is close to the limit or
  // if this tracker is not within a query's MemTracker hierarchy.
  if (process_capacity < failed_allocation_size || query_tracker == nullptr) {
    // IMPALA-5598: For performance reasons, limit the levels of recursion when
    // dumping the process tracker to only two layers.
    ss << process_tracker->LogUsage(PROCESS_MEMTRACKER_LIMITED_DEPTH);
  }

  Status status = Status::MemLimitExceeded(ss.str());
  if (state != nullptr) state->LogError(status.msg());
  return status;
}

void MemTracker::AddGcFunction(GcFunction f) {
  gc_functions_.push_back(f);
}

bool MemTracker::LimitExceededSlow(MemLimit mode) {
  if (mode == MemLimit::HARD && bytes_over_limit_metric_ != nullptr) {
    bytes_over_limit_metric_->SetValue(consumption() - limit_);
  }
  return GcMemory(GetLimit(mode));
}

bool MemTracker::GcMemory(int64_t max_consumption) {
  if (max_consumption < 0) return true;
  lock_guard<mutex> l(gc_lock_);
  if (consumption_metric_ != NULL) RefreshConsumptionFromMetric();
  int64_t pre_gc_consumption = consumption();
  // Check if someone gc'd before us
  if (pre_gc_consumption < max_consumption) return false;
  if (num_gcs_metric_ != NULL) num_gcs_metric_->Increment(1);

  int64_t curr_consumption = pre_gc_consumption;
  // Try to free up some memory
  for (int i = 0; i < gc_functions_.size(); ++i) {
    // Try to free up the amount we are over plus some extra so that we don't have to
    // immediately GC again. Don't free all the memory since that can be unnecessarily
    // expensive.
    const int64_t EXTRA_BYTES_TO_FREE = 512L * 1024L * 1024L;
    int64_t bytes_to_free = curr_consumption - max_consumption + EXTRA_BYTES_TO_FREE;
    gc_functions_[i](bytes_to_free);
    if (consumption_metric_ != NULL) RefreshConsumptionFromMetric();
    curr_consumption = consumption();
    if (max_consumption - curr_consumption <= EXTRA_BYTES_TO_FREE) break;
  }

  if (bytes_freed_by_last_gc_metric_ != NULL) {
    bytes_freed_by_last_gc_metric_->SetValue(pre_gc_consumption - curr_consumption);
  }
  return curr_consumption > max_consumption;
}
}
