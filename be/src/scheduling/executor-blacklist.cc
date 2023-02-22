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

#include "scheduling/executor-blacklist.h"

#include <algorithm>

#include "statestore/statestore.h"
#include "util/time.h"

DEFINE_bool(blacklisting_enabled, true,
    "(Advanced) If false, disables local blacklisting of executors by coordinators, "
    "which temporarily removes executors that appear to be problematic from scheduling "
    "decisions.");

namespace impala {

const int32_t ExecutorBlacklist::PROBATION_TIMEOUT_MULTIPLIER = 5;
const float ExecutorBlacklist::BLACKLIST_TIMEOUT_PADDING = 1.2;

bool ExecutorBlacklist::BlacklistingEnabled() {
  return FLAGS_blacklisting_enabled;
}

void ExecutorBlacklist::Blacklist(
    const BackendDescriptorPB& be_desc, const Status& cause) {
  DCHECK(BlacklistingEnabled());
  DCHECK(be_desc.has_backend_id());
  auto entry_it = executor_list_.find(be_desc.backend_id());
  if (entry_it != executor_list_.end()) {
    // If this executor was already blacklisted, it must be that two different queries
    // tried to blacklist it at about the same time, so just leave it as is.
    Entry& entry = entry_it->second;
    if (entry.state == State::ON_PROBATION) {
      // This executor was on probation, so re-blacklist it.
      int64_t probation_timeout = GetBlacklistTimeoutMs() * PROBATION_TIMEOUT_MULTIPLIER;
      int64_t current_time_ms = MonotonicMillis();
      int64_t elapsed = current_time_ms - entry.blacklist_time_ms;
      entry.state = State::BLACKLISTED;
      entry.blacklist_time_ms = current_time_ms;
      entry.cause = cause;

      // Since NeedsMaintenance() doesn't consider executors that can be removed from
      // probation, executors may stay on probation much longer than the timeout, so check
      // the timeout here.
      if (elapsed > probation_timeout * entry.num_consecutive_blacklistings) {
        // This executor should have already been taken off probation, so act like it was.
        entry.num_consecutive_blacklistings = 1;
      } else {
        ++entry.num_consecutive_blacklistings;
      }
    }
  } else {
    // This executor was not already on the list, create a new Entry for it.
    executor_list_.insert(
        make_pair(be_desc.backend_id(), Entry(be_desc, MonotonicMillis(), cause)));
  }
  VLOG(2) << "Blacklisted " << be_desc.address() << ", current blacklist: "
          << DebugString();
}

ExecutorBlacklist::State ExecutorBlacklist::FindAndRemove(
    const BackendDescriptorPB& be_desc) {
  auto remove_it = executor_list_.find(be_desc.backend_id());
  if (remove_it == executor_list_.end()) {
    // Executor wasn't on the blacklist.
    return NOT_BLACKLISTED;
  }
  State removed_state = remove_it->second.state;
  executor_list_.erase(remove_it);
  return removed_state;
}

bool ExecutorBlacklist::NeedsMaintenance() const {
  int64_t blacklist_timeout = GetBlacklistTimeoutMs();
  int64_t now = MonotonicMillis();
  for (auto entry_it : executor_list_) {
    const Entry& entry = entry_it.second;
    if (entry.state == State::BLACKLISTED) {
      int64_t elapsed = now - entry.blacklist_time_ms;
      if (elapsed > blacklist_timeout * entry.num_consecutive_blacklistings) {
        // This backend has passed the timeout and can be put on probation.
        return true;
      }
    }
  }
  return false;
}

void ExecutorBlacklist::Maintenance(std::list<BackendDescriptorPB>* probation_list) {
  int64_t blacklist_timeout = GetBlacklistTimeoutMs();
  int64_t probation_timeout = blacklist_timeout * PROBATION_TIMEOUT_MULTIPLIER;
  int64_t now = MonotonicMillis();
  auto entry_it = executor_list_.begin();
  while (entry_it != executor_list_.end()) {
    Entry& entry = entry_it->second;
    int64_t elapsed = now - entry.blacklist_time_ms;
    if (entry.state == State::BLACKLISTED) {
      // Check if we can take it off the blacklist and put it on probation.
      if (elapsed > blacklist_timeout * entry.num_consecutive_blacklistings) {
        LOG(INFO) << "Executor " << entry.be_desc.address()
                  << " passed the timeout and will be taken off the blacklist.";
        probation_list->push_back(entry.be_desc);
        entry.state = State::ON_PROBATION;
      }
      ++entry_it;
    } else {
      // Check if we can take it off probation.
      if (elapsed > probation_timeout * entry.num_consecutive_blacklistings) {
        entry_it = executor_list_.erase(entry_it);
      } else {
        ++entry_it;
      }
    }
  }
  VLOG(2) << "Completed blacklist maintenance. Current blacklist: " << DebugString();
}

bool ExecutorBlacklist::IsBlacklisted(
    const BackendDescriptorPB& be_desc, Status* cause, int64_t* time_remaining_ms) const {
  auto entry_it = executor_list_.find(be_desc.backend_id());
  if (entry_it != executor_list_.end()) {
    const Entry& entry = entry_it->second;
    if (entry.state == State::BLACKLISTED) {
      if (cause != nullptr) *cause = entry.cause;
      if (time_remaining_ms != nullptr) {
        int64_t elapsed_ms = MonotonicMillis() - entry.blacklist_time_ms;
        int64_t total_timeout_ms =
            GetBlacklistTimeoutMs() * entry.num_consecutive_blacklistings;
        *time_remaining_ms = total_timeout_ms - elapsed_ms;
      }
      return true;
    }
  }
  return false;
}

std::string ExecutorBlacklist::BlacklistToString() const {
  std::stringstream ss;
  for (auto entry_it : executor_list_) {
    const Entry& entry = entry_it.second;
    if (entry.state == State::BLACKLISTED) ss << entry.be_desc.address() << " ";
  }
  return ss.str();
}

std::string ExecutorBlacklist::DebugString() const {
  std::stringstream ss;
  ss << "ExecutorBlacklist[";
  for (auto entry_it : executor_list_) {
    const Entry& entry = entry_it.second;
    DCHECK(entry.state == BLACKLISTED || entry.state == ON_PROBATION);
    ss << entry.be_desc.address() << " ("
       << (entry.state == BLACKLISTED ? "blacklisted" : "on probation") << ") ";
  }
  ss << "]";
  return ss.str();
}

int64_t ExecutorBlacklist::GetBlacklistTimeoutMs() const {
  // We add some padding to the timeout to account for possible small, random delays in
  // statestore failure detection and to minimize the chance that an executor goes down,
  // is blacklisted, and then is put on probation and causes another failure right before
  // being removed from the cluster membership by a statestore update.
  return BLACKLIST_TIMEOUT_PADDING * Statestore::FailedExecutorDetectionTimeMs();
}

} // namespace impala
