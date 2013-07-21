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

MemTracker::MemTracker(
    int64_t byte_limit, const std::string& label, MemTracker* parent)
  : limit_(byte_limit),
    label_(label),
    parent_(parent),
    consumption_(&local_counter_),
    local_counter_(TCounterType::BYTES),
    enable_logging_(false),
    log_stack_(false) {
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
    enable_logging_(false),
    log_stack_(false) {
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
    tracker->query_id_ = id;
    uid_to_mem_trackers_[id] = tracker;
    return tracker;
  }
}

MemTracker::~MemTracker() {
  lock_guard<mutex> l(uid_to_mem_trackers_lock_);
  // Erase the weak ptr reference from the map.
  uid_to_mem_trackers_.erase(query_id_);
}

string MemTracker::LogUsage(const string& prefix) {
  stringstream ss;
  if (LimitExceeded()) {
    ss << prefix << label_ << ": memory limit exceeded. Limit="
       << PrettyPrinter::Print(limit_, TCounterType::BYTES)
       << " Consumption="
       << PrettyPrinter::Print(consumption(), TCounterType::BYTES);
  } else {
    ss << prefix << label_ << ": consumption="
       << PrettyPrinter::Print(consumption(), TCounterType::BYTES);
  }
  return ss.str();
}

string MemTracker::LogUsage(const vector<MemTracker*>& trackers) {
  vector<string> usage_strings;
  for (int i = 0; i < trackers.size(); ++i) {
    usage_strings.push_back(trackers[i]->LogUsage(i == 0 ? "" : "  "));
  }
  return join(usage_strings, "\n");
}

void MemTracker::LogUpdate(bool is_consume, int64_t bytes) {
  stringstream ss;
  ss << this << " " << (is_consume ? "Consume: " : "Release: ") << bytes
     << " Consumption: " << consumption_->current_value();
  if (log_stack_) ss << endl << GetStackTrace();
  LOG(ERROR) << ss.str();
}

}
