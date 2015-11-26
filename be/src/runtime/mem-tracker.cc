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

#include "runtime/mem-tracker.h"

#include <boost/algorithm/string/join.hpp>
#include <google/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "runtime/exec-env.h"
#include "resourcebroker/resource-broker.h"
#include "scheduling/query-resource-mgr.h"
#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/pretty-printer.h"
#include "util/uid-util.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace strings;

namespace impala {

const string MemTracker::COUNTER_NAME = "PeakMemoryUsage";
MemTracker::RequestTrackersMap MemTracker::request_to_mem_trackers_;
MemTracker::PoolTrackersMap MemTracker::pool_to_mem_trackers_;
mutex MemTracker::static_mem_trackers_lock_;

AtomicInt<int64_t> MemTracker::released_memory_since_gc_;

// Name for request pool MemTrackers. '$0' is replaced with the pool name.
const string REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT = "RequestPool=$0";

MemTracker::MemTracker(int64_t byte_limit, int64_t rm_reserved_limit, const string& label,
    MemTracker* parent, bool log_usage_if_zero)
  : limit_(byte_limit),
    rm_reserved_limit_(rm_reserved_limit),
    label_(label),
    parent_(parent),
    consumption_(&local_counter_),
    local_counter_(TUnit::BYTES),
    consumption_metric_(NULL),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    log_usage_if_zero_(log_usage_if_zero),
    query_resource_mgr_(NULL),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  if (parent != NULL) parent_->AddChildTracker(this);
  Init();
}

MemTracker::MemTracker(
    RuntimeProfile* profile, int64_t byte_limit, int64_t rm_reserved_limit,
    const std::string& label, MemTracker* parent)
  : limit_(byte_limit),
    rm_reserved_limit_(rm_reserved_limit),
    label_(label),
    parent_(parent),
    consumption_(profile->AddHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES)),
    local_counter_(TUnit::BYTES),
    consumption_metric_(NULL),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    log_usage_if_zero_(true),
    query_resource_mgr_(NULL),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  if (parent != NULL) parent_->AddChildTracker(this);
  Init();
}

MemTracker::MemTracker(UIntGauge* consumption_metric,
    int64_t byte_limit, int64_t rm_reserved_limit, const string& label)
  : limit_(byte_limit),
    rm_reserved_limit_(rm_reserved_limit),
    label_(label),
    parent_(NULL),
    consumption_(&local_counter_),
    local_counter_(TUnit::BYTES),
    consumption_metric_(consumption_metric),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    log_usage_if_zero_(true),
    query_resource_mgr_(NULL),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  Init();
}

void MemTracker::Init() {
  DCHECK_GE(limit_, -1);
  DCHECK(rm_reserved_limit_ == -1 || limit_ == -1 || rm_reserved_limit_ <= limit_);
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
  lock_guard<mutex> l(child_trackers_lock_);
  tracker->child_tracker_it_ = child_trackers_.insert(child_trackers_.end(), tracker);
}

void MemTracker::UnregisterFromParent() {
  DCHECK(parent_ != NULL);
  lock_guard<mutex> l(parent_->child_trackers_lock_);
  parent_->child_trackers_.erase(child_tracker_it_);
  child_tracker_it_ = parent_->child_trackers_.end();
}

MemTracker* MemTracker::GetRequestPoolMemTracker(const string& pool_name,
    MemTracker* parent) {
  DCHECK(!pool_name.empty());
  lock_guard<mutex> l(static_mem_trackers_lock_);
  PoolTrackersMap::iterator it = pool_to_mem_trackers_.find(pool_name);
  if (it != pool_to_mem_trackers_.end()) {
    MemTracker* tracker = it->second;
    DCHECK(pool_name == tracker->pool_name_);
    return tracker;
  } else {
    if (parent == NULL) return NULL;
    // First time this pool_name registered, make a new object.
    MemTracker* tracker = new MemTracker(-1, -1,
          Substitute(REQUEST_POOL_MEM_TRACKER_LABEL_FORMAT, pool_name),
          parent);
    tracker->auto_unregister_ = true;
    tracker->pool_name_ = pool_name;
    pool_to_mem_trackers_[pool_name] = tracker;
    return tracker;
  }
}

shared_ptr<MemTracker> MemTracker::GetQueryMemTracker(
    const TUniqueId& id, int64_t byte_limit, int64_t rm_reserved_limit, MemTracker* parent,
    QueryResourceMgr* res_mgr) {
  if (byte_limit != -1) {
    if (byte_limit > MemInfo::physical_mem()) {
      LOG(WARNING) << "Memory limit "
                   << PrettyPrinter::Print(byte_limit, TUnit::BYTES)
                   << " exceeds physical memory of "
                   << PrettyPrinter::Print(MemInfo::physical_mem(), TUnit::BYTES);
    }
    VLOG_QUERY << "Using query memory limit: "
               << PrettyPrinter::Print(byte_limit, TUnit::BYTES);
  }

  lock_guard<mutex> l(static_mem_trackers_lock_);
  RequestTrackersMap::iterator it = request_to_mem_trackers_.find(id);
  if (it != request_to_mem_trackers_.end()) {
    // Return the existing MemTracker object for this id, converting the weak ptr
    // to a shared ptr.
    shared_ptr<MemTracker> tracker = it->second.lock();
    DCHECK_EQ(tracker->limit_, byte_limit);
    DCHECK(id == tracker->query_id_);
    DCHECK(parent == tracker->parent_);
    return tracker;
  } else {
    // First time this id registered, make a new object.  Give a shared ptr to
    // the caller and put a weak ptr in the map.
    shared_ptr<MemTracker> tracker(new MemTracker(byte_limit, rm_reserved_limit,
        Substitute("Query($0) Limit", lexical_cast<string>(id)), parent));
    tracker->auto_unregister_ = true;
    tracker->query_id_ = id;
    request_to_mem_trackers_[id] = tracker;
    if (res_mgr != NULL) tracker->SetQueryResourceMgr(res_mgr);
    return tracker;
  }
}

MemTracker::~MemTracker() {
  lock_guard<mutex> l(static_mem_trackers_lock_);
  if (auto_unregister_) UnregisterFromParent();
  // Erase the weak ptr reference from the map.
  request_to_mem_trackers_.erase(query_id_);
  // Per-pool trackers should live the entire lifetime of the impalad process, but
  // remove the element from the map in case this changes in the future.
  pool_to_mem_trackers_.erase(pool_name_);
}

void MemTracker::RegisterMetrics(MetricGroup* metrics, const string& prefix) {
  num_gcs_metric_ = metrics->AddCounter<int64_t>(Substitute("$0.num-gcs", prefix), 0);

  // TODO: Consider a total amount of bytes freed counter
  bytes_freed_by_last_gc_metric_ = metrics->AddGauge<int64_t>(
      Substitute("$0.bytes-freed-by-last-gc", prefix), -1);

  bytes_over_limit_metric_ = metrics->AddGauge<int64_t>(
      Substitute("$0.bytes-over-limit", prefix), -1);
}

// Calling this on the query tracker results in output like:
// Query Limit: memory limit exceeded. Limit=100.00 MB Consumption=106.19 MB
//   Fragment 5b45e83bbc2d92bd:d3ff8a7df7a2f491:  Consumption=52.00 KB
//     AGGREGATION_NODE (id=6):  Consumption=44.00 KB
//     EXCHANGE_NODE (id=5):  Consumption=0.00
//     DataStreamMgr:  Consumption=0.00
//   Fragment 5b45e83bbc2d92bd:d3ff8a7df7a2f492:  Consumption=100.00 KB
//     AGGREGATION_NODE (id=2):  Consumption=36.00 KB
//     AGGREGATION_NODE (id=4):  Consumption=40.00 KB
//     EXCHANGE_NODE (id=3):  Consumption=0.00
//     DataStreamMgr:  Consumption=0.00
//     DataStreamSender:  Consumption=16.00 KB
string MemTracker::LogUsage(const string& prefix) const {
  if (!log_usage_if_zero_ && consumption() == 0) return "";

  stringstream ss;
  ss << prefix << label_ << ":";
  if (CheckLimitExceeded()) ss << " memory limit exceeded.";
  if (limit_ > 0) ss << " Limit=" << PrettyPrinter::Print(limit_, TUnit::BYTES);
  if (rm_reserved_limit_ > 0) {
    ss << " RM Limit=" << PrettyPrinter::Print(rm_reserved_limit_, TUnit::BYTES);
  }
  ss << " Consumption=" << PrettyPrinter::Print(consumption(), TUnit::BYTES);

  stringstream prefix_ss;
  prefix_ss << prefix << "  ";
  string new_prefix = prefix_ss.str();
  lock_guard<mutex> l(child_trackers_lock_);
  string child_trackers_usage = LogUsage(new_prefix, child_trackers_);
  if (!child_trackers_usage.empty()) ss << "\n" << child_trackers_usage;
  return ss.str();
}

string MemTracker::LogUsage(const string& prefix, const list<MemTracker*>& trackers) {
  vector<string> usage_strings;
  for (list<MemTracker*>::const_iterator it = trackers.begin();
      it != trackers.end(); ++it) {
    string usage_string = (*it)->LogUsage(prefix);
    if (!usage_string.empty()) usage_strings.push_back(usage_string);
  }
  return join(usage_strings, "\n");
}

void MemTracker::LogUpdate(bool is_consume, int64_t bytes) const {
  stringstream ss;
  ss << this << " " << (is_consume ? "Consume: " : "Release: ") << bytes
     << " Consumption: " << consumption() << " Limit: " << limit_;
  if (log_stack_) ss << endl << GetStackTrace();
  LOG(ERROR) << ss.str();
}

bool MemTracker::GcMemory(int64_t max_consumption) {
  if (max_consumption < 0) return true;
  lock_guard<SpinLock> l(gc_lock_);
  if (consumption_metric_ != NULL) consumption_->Set(consumption_metric_->value());
  uint64_t pre_gc_consumption = consumption();
  // Check if someone gc'd before us
  if (pre_gc_consumption < max_consumption) return false;
  if (num_gcs_metric_ != NULL) num_gcs_metric_->Increment(1);

  // Try to free up some memory
  for (int i = 0; i < gc_functions_.size(); ++i) {
    gc_functions_[i]();
    if (consumption_metric_ != NULL) consumption_->Set(consumption_metric_->value());
    if (consumption() <= max_consumption) break;
  }

  if (bytes_freed_by_last_gc_metric_ != NULL) {
    bytes_freed_by_last_gc_metric_->set_value(pre_gc_consumption - consumption());
  }
  return consumption() > max_consumption;
}

void MemTracker::GcTcmalloc() {
#ifndef ADDRESS_SANITIZER
  released_memory_since_gc_ = 0;
  MallocExtension::instance()->ReleaseFreeMemory();
#else
  // Nothing to do if not using tcmalloc.
#endif
}

bool MemTracker::ExpandRmReservation(int64_t bytes) {
  if (query_resource_mgr_ == NULL || rm_reserved_limit_ == -1) return false;
  // TODO: Make this asynchronous after IO mgr changes to use TryConsume() are done.
  lock_guard<mutex> l(resource_acquisition_lock_);
  int64_t requested = consumption_->current_value() + bytes;
  // Can't exceed the hard limit under any circumstance
  if (requested >= limit_ && limit_ != -1) return false;
  // Test to see if we can satisfy the limit anyhow; maybe a different request was already
  // in flight.
  if (requested < rm_reserved_limit_) return true;

  int64_t bytes_allocated;
  Status status = query_resource_mgr_->RequestMemExpansion(bytes, &bytes_allocated);
  if (!status.ok()) {
    LOG(INFO) << "Failed to expand memory limit by "
              << PrettyPrinter::Print(bytes, TUnit::BYTES) << ": "
              << status.GetDetail();
    return false;
  }

  BOOST_FOREACH(const MemTracker* tracker, limit_trackers_) {
    if (tracker == this) continue;
    if (tracker->consumption_->current_value() + bytes_allocated > tracker->limit_) {
      // TODO: Allocation may be larger than needed and might exceed some parent
      // tracker limit. IMPALA-2182.
      VLOG_RPC << "Failed to use " << bytes_allocated << " bytes allocated over "
               << tracker->label() << " tracker limit=" << tracker->limit_
               << " consumption=" << tracker->consumption();
      // Don't adjust our limit; rely on query tear-down to release the resource.
      return false;
    }
  }

  rm_reserved_limit_ += bytes_allocated;
  // Resource broker might give us more than we ask for
  if (limit_ != -1) rm_reserved_limit_ = min(rm_reserved_limit_, limit_);
  VLOG_RPC << "Reservation bytes_allocated=" << bytes_allocated << " rm_reserved_limit="
           << rm_reserved_limit_ << " limit=" << limit_;
  return true;
}

}
