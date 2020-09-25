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

#pragma once

#include <unordered_map>
#include <vector>

#include "gen-cpp/statestore_service.pb.h"
#include "util/container-util.h"
#include "util/network-util.h"
#include "util/unique-id-hash.h"

namespace impala {

/// Class to maintain a local blacklist of executors.
///
/// Executors can be added to the blacklist with Blacklist() and removed with
/// FindAndRemove(). Users must periodically call Maintenance(), which updates the
/// blacklist according to some timeouts.
///
/// There is a timeout for executors on the blacklist, defined as the amount of time the
/// statestore takes to consider an executor down after it stops responding to heartbeats,
/// plus some padding. When a blacklisted executor has passed this timeout and
/// Maintenance() is called, the executor is put on 'probation'. This allows us to track
/// executors that have been repeatedly blacklisted in a short amount of time. Executors
/// are only removed from the blacklist and put on probation during Maintenance().
///
/// There is a timeout for probation, which is much longer than the blacklist timeout.
/// When an executor has been on probation for longer than this timeout and Maintenance()
/// is called, the executor will be taken off probation and completely removed from
/// 'executor_list_'. If an executor that was on probation is re-blacklisted, its timeout
/// for getting back off the blacklist or probation is multiplied by the number of times
/// it has been re-blacklisted since the last time it was neither blacklisted nor on
/// probation.
///
/// FindAndRemove() removes executors whether they are blacklisted or on probation. If the
/// executor was previously blacklisted, it is not put on probation. This can be used when
/// the cluster membership is updated by the statestore to fully remove an executor that
/// is no longer part of the cluster membership.
///
/// This class is not thread-safe.
class ExecutorBlacklist {
 public:
  // Represents the blacklisting states an executor can be in.
  enum State { NOT_BLACKLISTED, BLACKLISTED, ON_PROBATION };

  /// Returns true if blacklisting is enabled.
  static bool BlacklistingEnabled();

  /// Adds an executor to the blacklist, if it is not already blacklisted. If the executor
  /// was on probation, updates its entry in 'executor_list_' accordingly. Should only be
  /// called if BlacklistingEnabled() is true. 'cause' is an error status indicating why
  /// the executor is being blacklisted.
  void Blacklist(const BackendDescriptorPB& be_desc, const Status& cause);

  /// Removes an executor from the blacklist or probation, if it is in 'executor_list_'.
  /// Does not put blacklisted executors on probation. Returns the executor's state prior
  /// to this call. Will return 'BLACKLISTED' for an executor that has passed the
  /// blacklist timeout if Maintenance() wasn't called since the timeout.
  State FindAndRemove(const BackendDescriptorPB& be_desc);

  /// Returns true if there are executors that have passed the blacklist timeout and can
  /// be removed from the blacklist. Note that this does not consider executors that can
  /// be removed from probation, even though Maintenance() also handles those executors.
  /// This is to avoid unnecessary updates to the cluster membership, since removing
  /// executors from probation doesn't actually affect the current membership.
  bool NeedsMaintenance() const;

  /// Performs maintenance on the blacklist - if an executor has been blacklisted for
  /// longer than the blacklist timeout it will be removed from the blacklisted, put on
  /// probation, and its descriptor will be returned in 'probation_list'. If an executor
  /// has been on probation for longer than the probation timeout, it will be taken off
  /// probation.
  void Maintenance(std::list<BackendDescriptorPB>* probation_list);

  /// If 'be_desc' is blacklisted, sets 'cause' to the error Status that caused this
  /// executor to be blacklisted, sets 'time_remaining_ms' to the amount of time the
  /// executor has left on the blacklist, and returns true.
  bool IsBlacklisted(const BackendDescriptorPB& be_desc, Status* cause = nullptr,
      int64_t* time_remaining_ms = nullptr) const;

  /// Returns a space-separated string of the addresses of executors that are currently
  /// blacklisted.
  std::string BlacklistToString() const;

  /// Returns a string representation of the blacklist for use in debugging.
  std::string DebugString() const;

 private:
  /// Info about an executor that is either blacklisted or on probabtion.
  struct Entry {
    Entry(const BackendDescriptorPB& be_desc, int64_t blacklist_time_ms,
        const Status& cause)
      : be_desc(be_desc),
        blacklist_time_ms(blacklist_time_ms),
        state(State::BLACKLISTED),
        num_consecutive_blacklistings(1),
        cause(cause) {}

    BackendDescriptorPB be_desc;

    /// The UnixMillis() of the last time this executor was blacklisted.
    int64_t blacklist_time_ms;

    /// Whether the executor is blacklisted or on probation.
    State state;

    /// Number of times that this executor has been blacklisted since the last time it was
    /// off probation.
    int32_t num_consecutive_blacklistings;

    /// Error status representing the reason the executor was blacklisted.
    Status cause;
  };

  /// Returns the base blacklist timeout in ms. This should be multiplied by
  /// 'num_consecutive_blacklistings' for a particular executor when checking if it has
  /// passed the timeout.
  int64_t GetBlacklistTimeoutMs() const;

  /// Map from executor backend_id to executor entry for those nodes which have been
  /// blacklisted. Note that the map contains executors that are either blacklisted or
  /// on probation.
  std::unordered_map<UniqueIdPB, Entry> executor_list_;

  /// The amount to multiply the blacklist timeout by for the probation timeout.
  static const int32_t PROBATION_TIMEOUT_MULTIPLIER;

  /// Multiplier for extra padding to add the the blacklist timeout.
  static const float BLACKLIST_TIMEOUT_PADDING;
};

} // namespace impala
