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

#include "util/debug-util.h"
#include "util/uid-util.h"

using namespace boost;
using namespace std;

namespace impala {

const string MemTracker::COUNTER_NAME = "PeakMemoryUsage";
MemTracker::MemTrackersMap MemTracker::uid_to_mem_trackers_;
mutex MemTracker::uid_to_mem_trackers_lock_;

MemTracker::MemTracker(int64_t byte_limit, const string& label, MemTracker* parent)
  : limit_(byte_limit),
    label_(label),
    parent_(parent),
    consumption_(&local_counter_),
    local_counter_(TCounterType::BYTES),
    consumption_metric_(NULL),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  if (parent != NULL) parent_->AddChildTracker(this);
  Init();
}

MemTracker::MemTracker(
    RuntimeProfile* profile, int64_t byte_limit,
    const std::string& label, MemTracker* parent)
  : limit_(byte_limit),
    label_(label),
    parent_(parent),
    consumption_(profile->AddHighWaterMarkCounter(COUNTER_NAME, TCounterType::BYTES)),
    local_counter_(TCounterType::BYTES),
    consumption_metric_(NULL),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  if (parent != NULL) parent_->AddChildTracker(this);
  Init();
}

MemTracker::MemTracker(Metrics::PrimitiveMetric<uint64_t>* consumption_metric,
                       int64_t byte_limit, const string& label)
  : limit_(byte_limit),
    label_(label),
    parent_(NULL),
    consumption_(&local_counter_),
    local_counter_(TCounterType::BYTES),
    consumption_metric_(consumption_metric),
    auto_unregister_(false),
    enable_logging_(false),
    log_stack_(false),
    num_gcs_metric_(NULL),
    bytes_freed_by_last_gc_metric_(NULL),
    bytes_over_limit_metric_(NULL) {
  Init();
}

void MemTracker::Init() {
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

shared_ptr<MemTracker> MemTracker::GetQueryMemTracker(
    const TUniqueId& id, int64_t byte_limit, MemTracker* parent) {
  lock_guard<mutex> l(uid_to_mem_trackers_lock_);
  MemTrackersMap::iterator it = uid_to_mem_trackers_.find(id);
  if (it != uid_to_mem_trackers_.end()) {
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
    shared_ptr<MemTracker> tracker(new MemTracker(byte_limit, "Query Limit", parent));
    tracker->auto_unregister_ = true;
    tracker->query_id_ = id;
    uid_to_mem_trackers_[id] = tracker;
    return tracker;
  }
}

MemTracker::~MemTracker() {
  lock_guard<mutex> l(uid_to_mem_trackers_lock_);
  if (auto_unregister_) UnregisterFromParent();
  // Erase the weak ptr reference from the map.
  uid_to_mem_trackers_.erase(query_id_);
}

void MemTracker::RegisterMetrics(Metrics* metrics, const string& prefix) {
  stringstream num_gcs_key;
  num_gcs_key << prefix << ".num-gcs";
  num_gcs_metric_ = metrics->CreateAndRegisterPrimitiveMetric(num_gcs_key.str(), 0L);

  stringstream bytes_freed_by_last_gc_key;
  bytes_freed_by_last_gc_key << prefix << ".bytes-freed-by-last-gc";
  bytes_freed_by_last_gc_metric_ = metrics->RegisterMetric(
      new Metrics::BytesMetric(bytes_freed_by_last_gc_key.str(), -1));

  stringstream bytes_over_limit_key;
  bytes_over_limit_key << prefix << ".bytes-over-limit";
  bytes_over_limit_metric_ = metrics->RegisterMetric(
      new Metrics::BytesMetric(bytes_over_limit_key.str(), -1));
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
  stringstream ss;
  ss << prefix << label_ << ":";
  if (CheckLimitExceeded()) ss << " memory limit exceeded.";
  if (limit_ > 0) ss << " Limit=" << PrettyPrinter::Print(limit_, TCounterType::BYTES);
  ss << " Consumption=" << PrettyPrinter::Print(consumption(), TCounterType::BYTES);

  stringstream prefix_ss;
  prefix_ss << prefix << "  ";
  string new_prefix = prefix_ss.str();
  lock_guard<mutex> l(child_trackers_lock_);
  if (!child_trackers_.empty()) ss << "\n" << LogUsage(new_prefix, child_trackers_);
  return ss.str();
}

string MemTracker::LogUsage(const string& prefix, const list<MemTracker*>& trackers) {
  vector<string> usage_strings;
  for (list<MemTracker*>::const_iterator it = trackers.begin();
      it != trackers.end(); ++it) {
    usage_strings.push_back((*it)->LogUsage(prefix));
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
  DCHECK_GE(max_consumption, 0);
  ScopedSpinLock l(&gc_lock_);
  uint64_t pre_gc_consumption = consumption();
  // Check if someone gc'd before us
  if (pre_gc_consumption < max_consumption) return false;
  if (num_gcs_metric_ != NULL) num_gcs_metric_->Increment(1);

  // Try to free up some memory
  for (int i = 0; i < gc_functions_.size(); ++i) {
    gc_functions_[i]();
    if (consumption() < max_consumption) break;
  }

  if (bytes_freed_by_last_gc_metric_ != NULL) {
    bytes_freed_by_last_gc_metric_->Update(pre_gc_consumption - consumption());
  }
  return consumption() > max_consumption;
}

}
