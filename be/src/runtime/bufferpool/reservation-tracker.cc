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

#include "common/object-pool.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-tracker.h"
#include "util/dummy-runtime-profile.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

ReservationTracker::ReservationTracker() : initialized_(false), mem_tracker_(NULL) {}

ReservationTracker::~ReservationTracker() {
  DCHECK(!initialized_);
}

void ReservationTracker::InitRootTracker(
    RuntimeProfile* profile, int64_t reservation_limit) {
  lock_guard<SpinLock> l(lock_);
  DCHECK(!initialized_);
  parent_ = NULL;
  mem_tracker_ = NULL;
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
  DCHECK(parent != NULL);
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

  if (mem_tracker_ != NULL) {
    MemTracker* parent_mem_tracker = GetParentMemTracker();
    if (parent_mem_tracker != NULL) {
      // Make sure the parent links of the MemTrackers correspond to our parent links.
      DCHECK_EQ(parent_mem_tracker, mem_tracker_->parent());
      // Make sure we don't have a lower limit than the ancestor, since we don't enforce
      // limits at lower links.
      DCHECK_EQ(mem_tracker_->lowest_limit(), parent_mem_tracker->lowest_limit());
    } else {
      // Make sure we didn't leave a gap in the links. E.g. this tracker's grandparent
      // shouldn't have a MemTracker.
      ReservationTracker* ancestor = parent_;
      while (ancestor != NULL) {
        DCHECK(ancestor->mem_tracker_ == NULL);
        ancestor = ancestor->parent_;
      }
    }
  }

  InitCounters(profile, reservation_limit_);

  CheckConsistency();
}

void ReservationTracker::InitCounters(
    RuntimeProfile* profile, int64_t reservation_limit) {
  bool profile_provided = profile != NULL;
  if (profile == NULL) {
    dummy_profile_.reset(new DummyProfile);
    profile = dummy_profile_->profile();
  }

  // Check that another tracker's counters aren't already registered in the profile.
  DCHECK(profile->GetCounter("BufferPoolInitialReservation") == NULL);
  counters_.reservation_limit =
      ADD_COUNTER(profile, "BufferPoolReservationLimit", TUnit::BYTES);
  counters_.peak_reservation =
      profile->AddHighWaterMarkCounter("BufferPoolPeakReservation", TUnit::BYTES);
  counters_.peak_used_reservation =
      profile->AddHighWaterMarkCounter("BufferPoolPeakUsedReservation", TUnit::BYTES);

  COUNTER_SET(counters_.reservation_limit, reservation_limit);

  if (mem_tracker_ != NULL && profile_provided) {
    mem_tracker_->EnableReservationReporting(counters_);
  }
}

void ReservationTracker::Close() {
  lock_guard<SpinLock> l(lock_);
  if (!initialized_) return;
  CheckConsistency();
  DCHECK_EQ(used_reservation_, 0);
  DCHECK_EQ(child_reservations_, 0);
  // Release any reservation to parent.
  if (parent_ != NULL) DecreaseReservationInternalLocked(reservation_, false);
  mem_tracker_ = NULL;
  parent_ = NULL;
  initialized_ = false;
}

bool ReservationTracker::IncreaseReservation(int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
  return IncreaseReservationInternalLocked(bytes, false, false);
}

bool ReservationTracker::IncreaseReservationToFit(int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
  return IncreaseReservationInternalLocked(bytes, true, false);
}

bool ReservationTracker::IncreaseReservationInternalLocked(
    int64_t bytes, bool use_existing_reservation, bool is_child_reservation) {
  DCHECK(initialized_);
  int64_t reservation_increase =
      use_existing_reservation ? max<int64_t>(0, bytes - unused_reservation()) : bytes;
  DCHECK_GE(reservation_increase, 0);

  bool granted;
  // Check if the increase is allowed, starting at the bottom of hierarchy.
  if (reservation_ + reservation_increase > reservation_limit_) {
    granted = false;
  } else if (reservation_increase == 0) {
    granted = true;
  } else {
    if (parent_ == NULL) {
      granted = true;
    } else {
      lock_guard<SpinLock> l(parent_->lock_);
      granted =
          parent_->IncreaseReservationInternalLocked(reservation_increase, true, true);
    }
    if (granted && !TryUpdateMemTracker(reservation_increase)) {
      granted = false;
      // Roll back changes to ancestors if MemTracker update fails.
      parent_->DecreaseReservationInternal(reservation_increase, true);
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

bool ReservationTracker::TryUpdateMemTracker(int64_t reservation_increase) {
  if (mem_tracker_ == NULL) return true;
  if (GetParentMemTracker() == NULL) {
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

void ReservationTracker::DecreaseReservation(int64_t bytes) {
  DecreaseReservationInternal(bytes, false);
}

void ReservationTracker::DecreaseReservationInternal(
    int64_t bytes, bool is_child_reservation) {
  lock_guard<SpinLock> l(lock_);
  DecreaseReservationInternalLocked(bytes, is_child_reservation);
}

void ReservationTracker::DecreaseReservationInternalLocked(
    int64_t bytes, bool is_child_reservation) {
  DCHECK(initialized_);
  DCHECK_GE(reservation_, bytes);
  if (bytes == 0) return;
  if (is_child_reservation) child_reservations_ -= bytes;
  UpdateReservation(-bytes);
  // The reservation should be returned up the tree.
  if (mem_tracker_ != NULL) {
    if (GetParentMemTracker() == NULL) {
      mem_tracker_->Release(bytes);
    } else {
      mem_tracker_->ReleaseLocal(bytes, GetParentMemTracker());
    }
  }
  if (parent_ != NULL) parent_->DecreaseReservationInternal(bytes, true);
  CheckConsistency();
}

void ReservationTracker::AllocateFrom(int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
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
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  return reservation_;
}

int64_t ReservationTracker::GetUsedReservation() {
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  return used_reservation_;
}

int64_t ReservationTracker::GetUnusedReservation() {
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  return unused_reservation();
}

int64_t ReservationTracker::GetChildReservations() {
  lock_guard<SpinLock> l(lock_);
  DCHECK(initialized_);
  return child_reservations_;
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
  DCHECK_EQ(reservation_limit_, counters_.reservation_limit->value());
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

  string parent_debug_string = parent_ == NULL ? "NULL" : parent_->DebugString();
  return Substitute(
      "<ReservationTracker>: reservation_limit $0 reservation $1 used_reservation $2 "
      "child_reservations $3 parent:\n$4",
      reservation_limit_, reservation_, used_reservation_, child_reservations_,
      parent_debug_string);
}
}
