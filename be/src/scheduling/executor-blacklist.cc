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

void ExecutorBlacklist::Blacklist(const TBackendDescriptor& be_desc) {
  DCHECK(BlacklistingEnabled());
  DCHECK(!be_desc.ip_address.empty());
  vector<Entry>& be_descs = executor_list_[be_desc.ip_address];
  auto it = find_if(be_descs.begin(), be_descs.end(),
      std::bind(eqBePort, be_desc, std::placeholders::_1));
  if (it != be_descs.end()) {
    // If this executor was already blacklisted, it must be that two different queries
    // tried to blacklist it at about the same time, so just leave it as is.
    if (it->state == State::ON_PROBATION) {
      // This executor was on probation, so re-blacklist it.
      it->state = State::BLACKLISTED;
      it->blacklist_time_ms = MonotonicMillis();

      int64_t probation_timeout = GetBlacklistTimeoutMs() * PROBATION_TIMEOUT_MULTIPLIER;
      int64_t elapsed = MonotonicMillis() - it->blacklist_time_ms;
      // Since NeedsMaintenance() doesn't consider executors that can be removed from
      // probation, executors may stay on probation much longer than the timeout, so check
      // the timeout here.
      if (elapsed > probation_timeout * it->num_consecutive_blacklistings) {
        // This executor should have already been taken off probation, so act like it was.
        it->num_consecutive_blacklistings = 1;
      } else {
        ++it->num_consecutive_blacklistings;
      }
    }
  } else {
    // This executor was not already on the list, create a new Entry for it.
    be_descs.emplace_back(be_desc, MonotonicMillis());
  }
  VLOG(2) << "Blacklisted " << TNetworkAddressToString(be_desc.address)
          << ", current blacklist: " << DebugString();
}

ExecutorBlacklist::State ExecutorBlacklist::FindAndRemove(
    const TBackendDescriptor& be_desc) {
  auto be_descs_it = executor_list_.find(be_desc.ip_address);
  if (be_descs_it == executor_list_.end()) {
    // Executor wasn't on the blacklist.
    return NOT_BLACKLISTED;
  }
  vector<Entry>& be_descs = be_descs_it->second;
  auto remove_it = find_if(be_descs.begin(), be_descs.end(),
      std::bind(eqBePort, be_desc, std::placeholders::_1));
  if (remove_it == be_descs.end()) {
    // Executor wasn't on the blacklist.
    return NOT_BLACKLISTED;
  }
  State removed_state = remove_it->state;
  be_descs.erase(remove_it);
  if (be_descs.empty()) {
    executor_list_.erase(be_descs_it);
  }
  return removed_state;
}

bool ExecutorBlacklist::NeedsMaintenance() const {
  int64_t blacklist_timeout = GetBlacklistTimeoutMs();
  int64_t now = MonotonicMillis();
  for (auto executor_it : executor_list_) {
    for (auto entry_it : executor_it.second) {
      if (entry_it.state == State::BLACKLISTED) {
        int64_t elapsed = now - entry_it.blacklist_time_ms;
        if (elapsed > blacklist_timeout * entry_it.num_consecutive_blacklistings) {
          // This backend has passed the timeout and can be put on probation.
          return true;
        }
      }
    }
  }
  return false;
}

void ExecutorBlacklist::Maintenance(std::list<TBackendDescriptor>* probation_list) {
  int64_t blacklist_timeout = GetBlacklistTimeoutMs();
  int64_t probation_timeout = blacklist_timeout * PROBATION_TIMEOUT_MULTIPLIER;
  int64_t now = MonotonicMillis();
  auto executor_it = executor_list_.begin();
  while (executor_it != executor_list_.end()) {
    auto entry_it = executor_it->second.begin();
    while (entry_it != executor_it->second.end()) {
      int64_t elapsed = now - entry_it->blacklist_time_ms;
      if (entry_it->state == State::BLACKLISTED) {
        // Check if we can take it off the blacklist and put it on probation.
        if (elapsed > blacklist_timeout * entry_it->num_consecutive_blacklistings) {
          LOG(INFO) << "Executor " << TNetworkAddressToString(entry_it->be_desc.address)
                    << " passed the timeout and will be taken off the blacklist.";
          probation_list->push_back(entry_it->be_desc);
          entry_it->state = State::ON_PROBATION;
        }
        ++entry_it;
      } else {
        // Check if we can take it off probation.
        if (elapsed > probation_timeout * entry_it->num_consecutive_blacklistings) {
          entry_it = executor_it->second.erase(entry_it);
        } else {
          ++entry_it;
        }
      }
    }
    if (executor_it->second.empty()) {
      executor_it = executor_list_.erase(executor_it);
    } else {
      ++executor_it;
    }
  }
  VLOG(2) << "Completed blacklist maintenance. Current blacklist: " << DebugString();
}

bool ExecutorBlacklist::IsBlacklisted(const TBackendDescriptor& be_desc) const {
  for (auto executor_it : executor_list_) {
    if (executor_it.first == be_desc.ip_address) {
      for (auto entry_it : executor_it.second) {
        if (entry_it.be_desc.address.port == be_desc.address.port
            && entry_it.state == State::BLACKLISTED) {
          return true;
        }
      }
    }
  }
  return false;
}

std::string ExecutorBlacklist::BlacklistToString() const {
  std::stringstream ss;
  for (auto executor_it : executor_list_) {
    for (auto entry_it : executor_it.second) {
      if (entry_it.state == State::BLACKLISTED) {
        ss << TNetworkAddressToString(entry_it.be_desc.address) << " ";
      }
    }
  }
  return ss.str();
}

std::string ExecutorBlacklist::DebugString() const {
  std::stringstream ss;
  ss << "ExecutorBlacklist[";
  for (auto executor_it : executor_list_) {
    for (auto entry_it : executor_it.second) {
      ss << TNetworkAddressToString(entry_it.be_desc.address) << " ("
         << (entry_it.state == BLACKLISTED ? "blacklisted" : "on probation") << ") ";
    }
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

bool ExecutorBlacklist::eqBePort(
    const TBackendDescriptor& be_desc, const Entry& existing) {
  // The IP addresses must already match, so it is sufficient to check the port.
  DCHECK_EQ(existing.be_desc.ip_address, be_desc.ip_address);
  return existing.be_desc.address.port == be_desc.address.port;
}

} // namespace impala
