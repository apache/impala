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

#include "runtime/bufferpool/reservation-tracker.h"

#include <algorithm>
#include <cstdlib>

#include "common/object-pool.h"
#include "gutil/atomicops.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/dummy-runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

ReservationTracker::ReservationTracker() {}

ReservationTracker::~ReservationTracker() {
  DCHECK(!initialized_);
}

void ReservationTracker::InitRootTracker(
    RuntimeProfile* profile, int64_t reservation_limit) {
  lock_guard<SpinLock> l(lock_);
  DCHECK(!initialized_);
  parent_ = nullptr;
  mem_tracker_ = nullptr;
  reservation_limit_ = reservation_limit;
  reservation_ = 0;
  used_reservation_ = 0;
  child_reservations_ = 0;
  initialized_ = true;

  InitCounters(profile, reservation_limit_);
  COUNTER_SET(counters_.peak_reservation, reservation_);

  CheckConsistency();
}

void ReservationTracker::InitChildTracker(RuntimeProfile* profile,
    ReservationTracker* parent, MemTracker* mem_tracker, int64_t reservation_limit) {
  DCHECK(parent != nullptr);
  DCHECK_GE(reservation_limit, 0);

  lock_guard<SpinLock> l(lock_);
  DCHECK(!initialized_);
  parent_ = parent;
  mem_tracker_ = mem_tracker;

  reservation_limit_ = reservation_limit;
  reservation_ = 0;
  used_reservation_ = 0;
  child_reservations_ = 0;
  initialized_ = true;

  if (mem_tracker_ != nullptr) {
    MemTracker* parent_mem_tracker = GetParentMemTracker();
    if (parent_mem_tracker != nullptr) {
      // Make sure the parent links of the MemTrackers correspond to our parent links.
      DCHECK_EQ(parent_mem_tracker, mem_tracker_->parent());
      // Make sure we don't have a lower limit than the ancestor, since we don't enforce
      // limits at lower links.
      DCHECK_EQ(mem_tracker_->lowest_limit(), parent_mem_tracker->lowest_limit());
    } else {
      // Make sure we didn't leave a gap in the links. E.g. this tracker's grandparent
      // shouldn't have a MemTracker.
      ReservationTracker* ancestor = parent_;
      while (ancestor != nullptr) {
        DCHECK(ancestor->mem_tracker_ == nullptr);
        ancestor = ancestor->parent_;
      }
    }
  }

  InitCounters(profile, reservation_limit_);

  CheckConsistency();
}

void ReservationTracker::InitCounters(
    RuntimeProfile* profile, int64_t reservation_limit) {
  if (profile == nullptr) {
    dummy_profile_.reset(new DummyProfile);
    profile = dummy_profile_->profile();
  }

  // Check that another tracker's counters aren't already registered in the profile.
  DCHECK(profile->GetCounter("PeakReservation") == nullptr);
  counters_.peak_reservation =
      profile->AddHighWaterMarkCounter("PeakReservation", TUnit::BYTES);
  counters_.peak_used_reservation =
      profile->AddHighWaterMarkCounter("PeakUsedReservation", TUnit::BYTES);
  // Only show the limit if set.
  counters_.reservation_limit = nullptr;
  if (reservation_limit != numeric_limits<int64_t>::max()) {
    counters_.reservation_limit = ADD_COUNTER(profile, "ReservationLimit", TUnit::BYTES);
    COUNTER_SET(counters_.reservation_limit, reservation_limit);
  }
  if (mem_tracker_ != nullptr) mem_tracker_->EnableReservationReporting(counters_);
}

void ReservationTracker::Close() {
  lock_guard<SpinLock> l(lock_);
  if (!initialized_) return;
  CheckConsistency();
  DCHECK_EQ(used_reservation_, 0);
  DCHECK_EQ(child_reservations_, 0);
  // Release any reservation to parent.
  if (parent_ != nullptr) DecreaseReservationLocked(reservation_, false);
  mem_tracker_ = nullptr;
  parent_ = nullptr;
  initialized_ = false;
}

bool ReservationTracker::IncreaseReservation(int64_t bytes, Status* error_status) {
  lock_guard<SpinLock> l(lock_);
  return IncreaseReservationInternalLocked(bytes, false, false, error_status);
}

bool ReservationTracker::IncreaseReservationToFit(int64_t bytes, Status* error_status) {
  lock_guard<SpinLock> l(lock_);
  return IncreaseReservationInternalLocked(bytes, true, false, error_status);
}

bool ReservationTracker::IncreaseReservationToFitAndAllocate(
    int64_t bytes, Status* error_status) {
  lock_guard<SpinLock> l(lock_);
  if (!IncreaseReservationInternalLocked(bytes, true, false, error_status)) return false;
  AllocateFromLocked(bytes);
  return true;
}

bool ReservationTracker::IncreaseReservationInternalLocked(int64_t bytes,
    bool use_existing_reservation, bool is_child_reservation, Status* error_status) {
  DCHECK(initialized_);
  int64_t reservation_increase =
      use_existing_reservation ? max<int64_t>(0, bytes - unused_reservation()) : bytes;
  DCHECK_GE(reservation_increase, 0);

  bool granted;
  // Check if the increase is allowed, starting at the bottom of hierarchy.
  if (reservation_increase == 0) {
    granted = true;
  } else if (increase_deny_probability_ != 0.0
      && rand() < increase_deny_probability_ * (RAND_MAX + 1L)) {
    // Randomly deny reservation if requested. Use rand() to avoid needing to set up a RNG.
    // Should be good enough. If the probability is 0.0, this never triggers. If it is 1.0
    // it always triggers.
    granted = false;
    if (error_status != nullptr) {
      *error_status = Status::Expected(
          Substitute("Debug random failure mode is turned on: Reservation of $0 denied.",
              PrettyPrinter::Print(bytes, TUnit::BYTES)));
    }
  } else if (reservation_ + reservation_increase > reservation_limit_) {
    granted = false;
    if (error_status != nullptr) {
      MemTracker* mem_tracker = mem_tracker_;
      if (mem_tracker == nullptr) {
        // The ReservationTracker at the root does not have a reference to the top
        // level(process) MemTracker.
        mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
      }
      string error_msg = Substitute(
          "Failed to increase reservation by $0 because it would exceed the applicable "
          "reservation limit for the \"$1\" ReservationTracker: reservation_limit=$2 "
          "reservation=$3 used_reservation=$4 child_reservations=$5",
          PrettyPrinter::Print(bytes, TUnit::BYTES),
          mem_tracker == nullptr ? "Process" : mem_tracker->label(),
          PrettyPrinter::Print(reservation_limit_, TUnit::BYTES),
          PrettyPrinter::Print(reservation_, TUnit::BYTES),
          PrettyPrinter::Print(used_reservation_, TUnit::BYTES),
          PrettyPrinter::Print(child_reservations_, TUnit::BYTES));
      string top_n_queries = mem_tracker->LogTopNQueries(5);
      if (!top_n_queries.empty()) {
        error_msg = Substitute(
            "$0\nThe top 5 queries that allocated memory under this tracker are:\n$1",
            error_msg, top_n_queries);
      }
      *error_status = Status::Expected(error_msg);
    }
  } else if (parent_ == nullptr) {
    // No parent and no linked MemTracker - increase can be granted.
    DCHECK(mem_tracker_ == nullptr) << "Root cannot have linked MemTracker";
    granted = true;
  } else {
    {
      lock_guard<SpinLock> l(parent_->lock_);
      granted = parent_->IncreaseReservationInternalLocked(
          reservation_increase, true, true, error_status);
    }
    if (granted && !TryConsumeFromMemTracker(reservation_increase)) {
      granted = false;
      if (error_status != nullptr) {
        *error_status = mem_tracker_->MemLimitExceeded(nullptr,
            "Could not allocate memory while trying to increase reservation.",
            reservation_increase);
      }
      // Roll back changes to ancestors if MemTracker update fails.
      parent_->DecreaseReservation(reservation_increase, true);
    }
  }
  if (granted) {
    // The reservation was granted and state updated in all ancestors: we can modify
    // this tracker's state now.
    UpdateReservation(reservation_increase);
    if (is_child_reservation) child_reservations_ += bytes;
  }

  CheckConsistency();
  return granted;
}

bool ReservationTracker::TryConsumeFromMemTracker(int64_t reservation_increase) {
  DCHECK_GE(reservation_increase, 0);
  if (mem_tracker_ == nullptr) return true;
  if (GetParentMemTracker() == nullptr) {
    // At the topmost link, which may be a MemTracker with a limit, we need to use
    // TryConsume() to check the limit.
    return mem_tracker_->TryConsume(reservation_increase);
  } else {
    // For lower links, there shouldn't be a limit to enforce, so we just need to
    // update the consumption of the linked MemTracker since the reservation is
    // already reflected in its parent.
    mem_tracker_->ConsumeLocal(reservation_increase, GetParentMemTracker());
    return true;
  }
}

void ReservationTracker::ReleaseToMemTracker(int64_t reservation_decrease) {
  DCHECK_GE(reservation_decrease, 0);
  if (mem_tracker_ == nullptr) return;
  if (GetParentMemTracker() == nullptr) {
    mem_tracker_->Release(reservation_decrease);
  } else {
    mem_tracker_->ReleaseLocal(reservation_decrease, GetParentMemTracker());
  }
}

void ReservationTracker::DecreaseReservation(int64_t bytes, bool is_child_reservation) {
  lock_guard<SpinLock> l(lock_);
  DecreaseReservationLocked(bytes, is_child_reservation);
}

void ReservationTracker::DecreaseReservationLocked(
    int64_t bytes, bool is_child_reservation) {
  DCHECK(initialized_);
  DCHECK_GE(reservation_, bytes);
  if (bytes == 0) return;
  if (is_child_reservation) child_reservations_ -= bytes;
  UpdateReservation(-bytes);
  ReleaseToMemTracker(bytes);
  // The reservation should be returned up the tree.
  if (parent_ != nullptr) parent_->DecreaseReservation(bytes, true);
  CheckConsistency();
}

bool ReservationTracker::TransferReservationTo(ReservationTracker* other, int64_t bytes) {
  if (other == this) return true;
  // Find the path to the root from both. The root is guaranteed to be a common ancestor.
  vector<ReservationTracker*> path_to_common = FindPathToRoot();
  vector<ReservationTracker*> other_path_to_common = other->FindPathToRoot();
  DCHECK_EQ(path_to_common.back(), other_path_to_common.back());
  ReservationTracker* common_ancestor = path_to_common.back();
  // Remove any common ancestors - they do not need to be updated for this transfer.
  while (!path_to_common.empty() && !other_path_to_common.empty()
      && path_to_common.back() == other_path_to_common.back()) {
    common_ancestor = path_to_common.back();
    path_to_common.pop_back();
    other_path_to_common.pop_back();
  }

  // At this point, we have three cases:
  // 1. 'common_ancestor' == 'other'. 'other_path_to_common' is empty because 'other' is
  //    the lowest common ancestor. To transfer, we decrease the reservation on the
  //    trackers under 'other', down to 'this'.
  // 2. 'common_ancestor' == 'this'. 'path_to_common' is empty because 'this' is the
  //    lowest common ancestor. To transfer, we increase the reservation on the trackers
  //    under 'this', down to 'other'.
  // 3. Neither is an ancestor of the other. Both 'other_path_to_common' and
  //    'path_to_common' are non-empty. We increase the reservation on trackers from
  //    'other' up to one below the common ancestor (checking limits as needed) and if
  //    successful, decrease reservations on trackers from 'this' up to one below the
  //    common ancestor.

  // Lock all of the trackers so we can do the update atomically. Need to be careful to
  // lock subtrees in the correct order.
  vector<unique_lock<SpinLock>> locks;
  bool lock_first = path_to_common.empty() || other_path_to_common.empty()
      || lock_sibling_subtree_first(path_to_common.back(), other_path_to_common.back());
  if (lock_first) {
    for (ReservationTracker* tracker : path_to_common) locks.emplace_back(tracker->lock_);
  }
  for (ReservationTracker* tracker : other_path_to_common) {
    locks.emplace_back(tracker->lock_);
  }
  if (!lock_first) {
    for (ReservationTracker* tracker : path_to_common) locks.emplace_back(tracker->lock_);
  }

  // Check reservation limits will not be violated before applying any updates.
  for (ReservationTracker* tracker : other_path_to_common) {
    if (tracker->reservation_ + bytes > tracker->reservation_limit_) return false;
  }

  // Do the updates now that we have checked the limits. We're holding all the locks
  // so this is all atomic.
  for (ReservationTracker* tracker : other_path_to_common) {
    tracker->UpdateReservation(bytes);
    // We don't handle MemTrackers with limit in this function - this should always
    // succeed.
    DCHECK(tracker->mem_tracker_ == nullptr || !tracker->mem_tracker_->has_limit());
    bool success = tracker->TryConsumeFromMemTracker(bytes);
    DCHECK(success);
    if (tracker != other_path_to_common[0]) tracker->child_reservations_ += bytes;
  }
  for (ReservationTracker* tracker : path_to_common) {
    if (tracker != path_to_common[0]) tracker->child_reservations_ -= bytes;
    tracker->UpdateReservation(-bytes);
    tracker->ReleaseToMemTracker(bytes);
  }

  // Update the 'child_reservations_' on the common ancestor if needed.
  // Case 1: reservation was pushed up to 'other'.
  if (common_ancestor == other) {
    lock_guard<SpinLock> l(other->lock_);
    other->child_reservations_ -= bytes;
    other->CheckConsistency();
  }
  // Case 2: reservation was pushed down below 'this'.
  if (common_ancestor == this) {
    lock_guard<SpinLock> l(lock_);
    child_reservations_ += bytes;
    CheckConsistency();
  }
  return true;
}

vector<ReservationTracker*> ReservationTracker::FindPathToRoot() {
  vector<ReservationTracker*> path_to_root;
  ReservationTracker* curr = this;
  do {
    path_to_root.push_back(curr);
    curr = curr->parent_;
  } while (curr != nullptr);
  return path_to_root;
}

void ReservationTracker::AllocateFrom(int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
  AllocateFromLocked(bytes);
}

void ReservationTracker::AllocateFromLocked(int64_t bytes) {
  DCHECK(initialized_);
  DCHECK_GE(bytes, 0);
  DCHECK_LE(bytes, unused_reservation());
  UpdateUsedReservation(bytes);
  CheckConsistency();
}

void ReservationTracker::ReleaseTo(int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  DCHECK_GE(bytes, 0);
  DCHECK_LE(bytes, used_reservation_);
  UpdateUsedReservation(-bytes);
  CheckConsistency();
}

int64_t ReservationTracker::GetReservation() {
  // Don't acquire lock - there is no point in holding it for this function only since
  // the value read can change as soon as we release it.
  DCHECK(initialized_);
  return base::subtle::Acquire_Load(&reservation_);
}

int64_t ReservationTracker::GetUsedReservation() {
  // Don't acquire lock - there is no point in holding it for this function only since
  // the value read can change as soon as we release it.
  DCHECK(initialized_);
  return base::subtle::Acquire_Load(&used_reservation_);
}

int64_t ReservationTracker::GetUnusedReservation() {
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  return unused_reservation();
}

int64_t ReservationTracker::GetChildReservations() {
  // Don't acquire lock - there is no point in holding it for this function only since
  // the value read can change as soon as we release it.
  DCHECK(initialized_);
  return base::subtle::Acquire_Load(&child_reservations_);
}

void ReservationTracker::CheckConsistency() const {
  // Check internal invariants.
  DCHECK_GE(reservation_, 0);
  DCHECK_LE(reservation_, reservation_limit_);
  DCHECK_GE(child_reservations_, 0);
  DCHECK_GE(used_reservation_, 0);
  DCHECK_LE(used_reservation_ + child_reservations_, reservation_);

  DCHECK_EQ(reservation_, counters_.peak_reservation->current_value());
  DCHECK_LE(reservation_, counters_.peak_reservation->value());
  DCHECK_EQ(used_reservation_, counters_.peak_used_reservation->current_value());
  DCHECK_LE(used_reservation_, counters_.peak_used_reservation->value());
  if (counters_.reservation_limit != nullptr) {
    DCHECK_EQ(reservation_limit_, counters_.reservation_limit->value());
  }
}

void ReservationTracker::UpdateUsedReservation(int64_t delta) {
  used_reservation_ += delta;
  COUNTER_SET(counters_.peak_used_reservation, used_reservation_);
  CheckConsistency();
}

void ReservationTracker::UpdateReservation(int64_t delta) {
  reservation_ += delta;
  COUNTER_SET(counters_.peak_reservation, reservation_);
  CheckConsistency();
}

string ReservationTracker::DebugString() {
  lock_guard<SpinLock> l(lock_);
  if (!initialized_) return "<ReservationTracker>: uninitialized";

  string parent_debug_string = parent_ == nullptr ? "NULL" : parent_->DebugString();
  return Substitute(
      "<ReservationTracker>: reservation_limit $0 reservation $1 used_reservation $2 "
      "child_reservations $3 parent:\n$4",
      reservation_limit_, reservation_, used_reservation_, child_reservations_,
      parent_debug_string);
}
}
