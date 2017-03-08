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

#ifndef IMPALA_RUNTIME_RESERVATION_TRACKER_H
#define IMPALA_RUNTIME_RESERVATION_TRACKER_H

#include <stdint.h>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>
#include <string>

#include "runtime/bufferpool/reservation-tracker-counters.h"
#include "common/status.h"
#include "util/spinlock.h"

namespace impala {

class DummyProfile;
class MemTracker;
class RuntimeProfile;

/// A tracker for a hierarchy of buffer pool memory reservations, denominated in bytes.
/// A hierarchy of ReservationTrackers provides a mechanism for subdividing buffer pool
/// memory and enforcing upper and lower bounds on memory usage.
///
/// The root of the tracker tree enforces a global maximum, which is distributed among its
/// children. Each tracker in the tree has a 'reservation': the total bytes of buffer pool
/// memory it is entitled to use. The reservation is inclusive of any memory that is
/// already allocated from the reservation, i.e. using a reservation to allocate memory
/// does not subtract from the reservation.
///
/// A reservation can be used directly at the tracker by calling AllocateFrom(), or
/// distributed to children of the tracker for the childrens' reservations. Each tracker
/// in the tree can use up to its reservation without checking parent trackers. To
/// increase its reservation, a tracker must use some of its parent's reservation (and
/// perhaps increase reservations all the way to the root of the tree).
///
/// Each tracker also has a maximum reservation that is enforced. E.g. if the root of the
/// tracker hierarchy is the global tracker for the Impala daemon and the next level of
/// the hierarchy is made up of per-query trackers, then the maximum reservation
/// mechanism can enforce both process-level and query-level limits on reservations.
///
/// Invariants:
/// * A tracker's reservation is at most its reservation limit: reservation <= limit
/// * A tracker's reservation is at least the sum of its childrens' reservations plus
///   the amount of the reservation used directly at this tracker. The difference is
///   the unused reservation:
///     child_reservations + used_reservation + unused_reservation = reservation.
///
/// Thread-safety:
/// All public ReservationTracker methods are thread-safe. If multiple threads
/// concurrently invoke methods on a ReservationTracker, each operation is applied
/// atomically to leave the ReservationTracker in a consistent state. Calling threads
/// are responsible for coordinating to avoid violating any method preconditions,
/// e.g. ensuring that there is sufficient unused reservation before calling AllocateTo().
///
/// Integration with MemTracker hierarchy:
/// TODO: we will remove MemTracker and this integration once all memory is accounted via
/// reservations.
///
/// Each ReservationTracker can optionally have a linked MemTracker. E.g. an exec
/// node's ReservationTracker can be linked with the exec node's MemTracker, so that
/// reservations are included in query memory consumption for the purposes of enforcing
/// memory limits, reporting and logging. The reservation is accounted as consumption
/// against the linked MemTracker and its ancestors because reserved memory is committed.
/// Allocating from a reservation therefore does not change the consumption reflected in
/// the MemTracker hierarchy.
///
/// MemTracker limits are only checked via the topmost link (i.e. the query-level
/// trackers): we require that no MemTrackers below this level have limits.
///
/// We require that the MemTracker hierarchy is consistent with the ReservationTracker
/// hierarchy. I.e. if a ReservationTracker is linked to a MemTracker "A", and its parent
/// is linked to a MemTracker "B", then "B" must be the parent of "A"'.
class ReservationTracker {
 public:
  ReservationTracker();
  virtual ~ReservationTracker();

  /// Initializes the root tracker with the given reservation limit in bytes. The initial
  /// reservation is 0.
  /// if 'profile' is not NULL, the counters defined in ReservationTrackerCounters are
  /// added to 'profile'.
  void InitRootTracker(RuntimeProfile* profile, int64_t reservation_limit);

  /// Initializes a new ReservationTracker with a parent.
  /// If 'mem_tracker' is not NULL, reservations for this ReservationTracker and its
  /// children will be counted as consumption against 'mem_tracker'.
  /// 'reservation_limit' is the maximum reservation for this tracker in bytes.
  /// if 'profile' is not NULL, the counters in 'counters_' are added to 'profile'.
  void InitChildTracker(RuntimeProfile* profile, ReservationTracker* parent,
      MemTracker* mem_tracker, int64_t reservation_limit);

  /// If the tracker is initialized, deregister the ReservationTracker from its parent,
  /// relinquishing all this tracker's reservation. All of the reservation must be unused
  /// and all the tracker's children must be closed before calling this method.
  /// TODO: decide on and implement policy for how far to release the reservation up
  /// the tree. Currently the reservation is released all the way to the root.
  void Close();

  /// Request to increase reservation by 'bytes'. The request is either granted in
  /// full or not at all. Uses any unused reservation on ancestors and increase
  /// ancestors' reservations if needed to fit the increased reservation.
  /// Returns true if the reservation increase is granted, or false if not granted.
  /// If the reservation is not granted, no modifications are made to the state of
  /// any ReservationTrackers.
  bool IncreaseReservation(int64_t bytes) WARN_UNUSED_RESULT;

  /// Tries to ensure that 'bytes' of unused reservation is available. If not already
  /// available, tries to increase the reservation such that the unused reservation is
  /// exactly equal to 'bytes'. Uses any unused reservation on ancestors and increase
  /// ancestors' reservations if needed to fit the increased reservation.
  /// Returns true if the reservation increase was successful or not necessary.
  bool IncreaseReservationToFit(int64_t bytes) WARN_UNUSED_RESULT;

  /// Decrease tracker's reservation by 'bytes'. This tracker's reservation must be at
  /// least 'bytes' before calling this method.
  /// TODO: decide on and implement policy for how far to release the reservation up
  /// the tree. Currently the reservation is released all the way to the root.
  void DecreaseReservation(int64_t bytes);

  /// Allocate 'bytes' from the reservation. The tracker must have at least 'bytes'
  /// unused reservation before calling this method.
  void AllocateFrom(int64_t bytes);

  /// Release 'bytes' of previously allocated memory. The used reservation is
  /// decreased by 'bytes'. Before the call, the used reservation must be at least
  /// 'bytes' before calling this method.
  void ReleaseTo(int64_t bytes);

  /// Returns the amount of the reservation in bytes.
  int64_t GetReservation();

  /// Returns the current amount of the reservation used at this tracker, not including
  /// reservations of children in bytes.
  int64_t GetUsedReservation();

  /// Returns the amount of the reservation neither used nor given to childrens'
  /// reservations at this tracker in bytes.
  int64_t GetUnusedReservation();

  /// Returns the total reservations of children in bytes.
  int64_t GetChildReservations();

  std::string DebugString();

 private:
  /// Returns the amount of 'reservation_' that is unused.
  inline int64_t unused_reservation() const {
    return reservation_ - used_reservation_ - child_reservations_;
  }

  /// Returns the parent's memtracker if 'parent_' is non-NULL, or NULL otherwise.
  MemTracker* GetParentMemTracker() const {
    return parent_ == NULL ? NULL : parent_->mem_tracker_;
  }

  /// Initializes 'counters_', storing the counters in 'profile'.
  /// If 'profile' is NULL, creates a dummy profile to store the counters.
  void InitCounters(RuntimeProfile* profile, int64_t max_reservation);

  /// Internal helper for IncreaseReservation(). If 'use_existing_reservation' is true,
  /// increase by the minimum amount so that 'bytes' fits in the reservation, otherwise
  /// just increase by 'bytes'. If 'is_child_reservation' is true, also increase
  /// 'child_reservations_' by 'bytes'.
  /// 'lock_' must be held by caller.
  bool IncreaseReservationInternalLocked(
      int64_t bytes, bool use_existing_reservation, bool is_child_reservation);

  /// Update consumption on linked MemTracker. For the topmost link, return false if
  /// this failed because it would exceed a memory limit. If there is no linked
  /// MemTracker, just returns true.
  /// TODO: remove once we account all memory via ReservationTrackers.
  bool TryUpdateMemTracker(int64_t reservation_increase);

  /// Internal helper for DecreaseReservation(). This behaves the same as
  /// DecreaseReservation(), except when 'is_child_reservation' is true it decreases
  /// 'child_reservations_' by 'bytes'.
  void DecreaseReservationInternal(int64_t bytes, bool is_child_reservation);

  /// Same as DecreaseReservationInternal(), but 'lock_' must be held by caller.
  void DecreaseReservationInternalLocked(int64_t bytes, bool is_child_reservation);

  /// Check the internal consistency of the ReservationTracker and DCHECKs if in an
  /// inconsistent state.
  /// 'lock_' must be held by caller.
  void CheckConsistency() const;

  /// Increase or decrease 'used_reservation_' and update profile counters accordingly.
  /// 'lock_' must be held by caller.
  void UpdateUsedReservation(int64_t delta);

  /// Increase or decrease 'reservation_' and update profile counters accordingly.
  /// 'lock_' must be held by caller.
  void UpdateReservation(int64_t delta);

  /// lock_ protects all members. In a hierarchy of trackers, locks must be acquired
  /// from the bottom-up.
  SpinLock lock_;

  /// True if the tracker is initialized.
  bool initialized_;

  /// A dummy profile to hold the counters in 'counters_' in the case that no profile
  /// is provided.
  boost::scoped_ptr<DummyProfile> dummy_profile_;

  /// The RuntimeProfile counters for this tracker.
  /// All non-NULL if 'initialized_' is true.
  ReservationTrackerCounters counters_;

  /// The parent of this tracker in the hierarchy. Does not change after initialization.
  ReservationTracker* parent_;

  /// If non-NULL, reservations are counted as memory consumption against this tracker.
  /// Does not change after initialization. Not owned.
  /// TODO: remove once all memory is accounted via ReservationTrackers.
  MemTracker* mem_tracker_;

  /// The maximum reservation in bytes that this tracker can have.
  int64_t reservation_limit_;

  /// This tracker's current reservation in bytes. 'reservation_' <= 'reservation_limit_'.
  int64_t reservation_;

  /// Total reservation of children in bytes. This is included in 'reservation_'.
  /// 'used_reservation_' + 'child_reservations_' <= 'reservation_'.
  int64_t child_reservations_;

  /// The amount of the reservation currently used by this tracker in bytes.
  /// 'used_reservation_' + 'child_reservations_' <= 'reservation_'.
  int64_t used_reservation_;
};
}

#endif
