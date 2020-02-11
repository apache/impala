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
#include <string>

#include "common/status.h"
#include "runtime/bufferpool/reservation-tracker-counters.h"
#include "runtime/mem-tracker-types.h"
#include "util/spinlock.h"

namespace impala {

class DummyProfile;
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
  /// 'mem_limit_mode' determines whether reservation increases are checked against the
  /// soft or hard limit of 'mem_tracker'. If 'profile' is not NULL, the counters in
  /// 'counters_' are added to 'profile'.
  void InitChildTracker(RuntimeProfile* profile, ReservationTracker* parent,
      MemTracker* mem_tracker, int64_t reservation_limit,
      MemLimit mem_limit_mode = MemLimit::SOFT);

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
  /// any ReservationTrackers and if 'error_status' is non-null, it returns an
  /// appropriate status message in it.
  bool IncreaseReservation(int64_t bytes, Status* error_status = nullptr)
      WARN_UNUSED_RESULT;

  /// Tries to ensure that 'bytes' of unused reservation is available. If not already
  /// available, tries to increase the reservation such that the unused reservation is
  /// exactly equal to 'bytes'. Uses any unused reservation on ancestors and increase
  /// ancestors' reservations if needed to fit the increased reservation.
  /// Returns true if the reservation increase was successful or not necessary. Otherwise
  /// returns false and if 'error_status' is non-null, it returns an appropriate status
  /// message in it.
  bool IncreaseReservationToFit(
      int64_t bytes, Status* error_status = nullptr) WARN_UNUSED_RESULT;

  /// Like IncreaseReservationToFit(), except 'bytes' is also allocated from
  /// the reservation on success.
  bool IncreaseReservationToFitAndAllocate(
      int64_t bytes, Status* error_status = nullptr) WARN_UNUSED_RESULT;

  /// Decrease reservation by 'bytes' on this tracker and all ancestors. This tracker's
  /// reservation must be at least 'bytes' before calling this method.
  void DecreaseReservation(int64_t bytes) { DecreaseReservation(bytes, false); }

  /// Transfer reservation from this tracker to 'other'. Both trackers must be in the
  /// same query subtree of the hierarchy. One tracker can be the ancestor of the other,
  /// or they can share a common ancestor. The subtree root must be at the query level
  /// or below so that the transfer cannot cause a MemTracker limit to be exceeded
  /// (because linked MemTrackers with limits below the query level are not supported).
  /// Returns true on success or false if the transfer would have caused a reservation
  /// limit to be exceeded.
  bool TransferReservationTo(ReservationTracker* other, int64_t bytes) WARN_UNUSED_RESULT;

  /// Allocate 'bytes' from the reservation. The tracker must have at least 'bytes'
  /// unused reservation before calling this method.
  void AllocateFrom(int64_t bytes);

  /// Release 'bytes' of previously allocated memory. The used reservation is
  /// decreased by 'bytes'. Before the call, the used reservation must be at least
  /// 'bytes' before calling this method.
  void ReleaseTo(int64_t bytes);

  /// Returns the amount of the reservation in bytes. Does not acquire the internal lock.
  int64_t GetReservation();

  /// Returns the current amount of the reservation used at this tracker, not including
  /// reservations of children in bytes. Does not acquire the internal lock.
  int64_t GetUsedReservation();

  /// Returns the amount of the reservation neither used nor given to childrens'
  /// reservations at this tracker in bytes. Acquires the internal lock.
  int64_t GetUnusedReservation();

  /// Returns the total reservations of children in bytes. Does not acquire the
  /// internal lock.
  int64_t GetChildReservations();

  /// Support for debug actions: deny reservation increase with probability 'probability'.
  void SetDebugDenyIncreaseReservation(double probability) {
    increase_deny_probability_ = probability;
  }

  ReservationTracker* parent() const { return parent_; }

  std::string DebugString();

 private:
  /// Returns the amount of 'reservation_' that is unused.
  inline int64_t unused_reservation() const {
    return reservation_.Load() - used_reservation_.Load() - child_reservations_.Load();
  }

  /// Returns the parent's memtracker if 'parent_' is non-NULL, or NULL otherwise.
  MemTracker* GetParentMemTracker() const {
    return parent_ == nullptr ? nullptr : parent_->mem_tracker_;
  }

  /// Initializes 'counters_', storing the counters in 'profile'.
  /// If 'profile' is NULL, creates a dummy profile to store the counters.
  void InitCounters(RuntimeProfile* profile, int64_t max_reservation);

  /// Internal helper for IncreaseReservation(). If 'use_existing_reservation' is true,
  /// increase by the minimum amount so that 'bytes' fits in the reservation, otherwise
  /// just increase by 'bytes'. If 'is_child_reservation' is true, also increase
  /// 'child_reservations_' by 'bytes'. If 'error_status' is not null and reservation
  /// increase fails then an appropriate status message is returned in it.
  /// 'lock_' must be held by caller.
  /// Example error message if a reservation tracker hits its limit:
  /// Failed to increase reservation by 2.12 GB because it would exceed the applicable
  /// reservation limit for the "Process" ReservationTracker: reservation_limit=2.00 GB
  /// reservation=0 used_reservation=0 child_reservations=0
  /// The top 5 queries that allocated memory under this tracker are:
  /// Query(20449659107d67ce:2e9058b500000000): Reservation=0 ReservationLimit=6.67 GB
  /// OtherMemory=0 Total=0 Peak=0
  bool IncreaseReservationInternalLocked(int64_t bytes, bool use_existing_reservation,
      bool is_child_reservation, Status* error_status = nullptr);

  /// Increase consumption on linked MemTracker to reflect an increase in reservation
  /// of 'reservation_increase'. For the topmost link, return false if this failed
  /// because it would exceed a memory limit. If there is no linked MemTracker, just
  /// returns true.
  /// TODO: remove once we account all memory via ReservationTrackers.
  bool TryConsumeFromMemTracker(int64_t reservation_increase, MemLimit mem_limit_mode);

  /// Decrease consumption on linked MemTracker to reflect a decrease in reservation of
  /// 'reservation_decrease'. If there is no linked MemTracker, does nothing.
  /// TODO: remove once we account all memory via ReservationTrackers.
  void ReleaseToMemTracker(int64_t reservation_decrease);

  /// Decrease reservation by 'bytes' on this tracker and all ancestors. This tracker's
  /// reservation must be at least 'bytes' before calling this method. If
  /// 'is_child_reservation' is true it decreases 'child_reservations_' by 'bytes'
  void DecreaseReservation(int64_t bytes, bool is_child_reservation);

  /// Same as DecreaseReservation(), but 'lock_' must be held by caller.
  void DecreaseReservationLocked(int64_t bytes, bool is_child_reservation);

  /// Return a vector containing the trackers on the path to the root tracker. Includes
  /// the current tracker and the root tracker.
  std::vector<ReservationTracker*> FindPathToRoot();

  /// Return true if trackers in the subtree rooted at 'subtree1' precede trackers in
  /// the subtree rooted at 'subtree2' in the lock order. 'subtree1' and 'subtree2'
  /// must share the same parent.
  static bool lock_sibling_subtree_first(
      ReservationTracker* subtree1, ReservationTracker* subtree2) {
    DCHECK_EQ(subtree1->parent_, subtree2->parent_);
    return reinterpret_cast<uintptr_t>(subtree1) < reinterpret_cast<uintptr_t>(subtree2);
  }

  /// Check the internal consistency of the ReservationTracker and DCHECKs if in an
  /// inconsistent state.
  /// 'lock_' must be held by caller.
  void CheckConsistency() const;

  /// Same as AllocateFrom() except 'lock_' must be held by caller.
  void AllocateFromLocked(int64_t bytes);

  /// Increase or decrease 'used_reservation_' and update profile counters accordingly.
  /// 'lock_' must be held by caller.
  void UpdateUsedReservation(int64_t delta);

  /// Increase or decrease 'reservation_' and update profile counters accordingly.
  /// 'lock_' must be held by caller.
  void UpdateReservation(int64_t delta);

  /// Support for debug actions: see SetDebugDenyIncreaseReservation() for behaviour.
  double increase_deny_probability_ = 0.0;

  /// lock_ protects all below members. The lock order in a tree of ReservationTrackers is
  /// based on a post-order traversal of the tree, with children visited in order of the
  /// memory address of the ReservationTracker object. The following rules can be applied
  /// to determine the relative positions of two trackers t1 and t2 in the lock order:
  /// * If t1 is a descendant of t2, t1's lock must be acquired before t2's lock (i.e.
  ///   locks are acquired bottom-up).
  /// * If neither t1 or t2 is a descendant of the other, they must be in subtrees of
  ///   under a common ancestor. If the memory address of t1's subtree's root is less
  ///   than the memory address of t2's subtree's root, t1's lock must be acquired before
  ///   t2's lock. This check is implemented in lock_sibling_subtree_first().
  /// Since MemTracker::child_trackers_lock_ objects are acquired in a top-down lock
  /// order, if a MemTracker::child_trackers_lock_ is acquired while holding a lock_, any
  /// more calls to acquire a lock_ should not be made to avoid any deadlock that might
  /// occur due to ReservationTracker's bottom-up lock order.
  SpinLock lock_;

  /// True if the tracker is initialized.
  bool initialized_ = false;

  /// A dummy profile to hold the counters in 'counters_' in the case that no profile
  /// is provided.
  boost::scoped_ptr<DummyProfile> dummy_profile_;

  /// The RuntimeProfile counters for this tracker.
  /// All non-NULL if 'initialized_' is true.
  ReservationTrackerCounters counters_;

  /// The parent of this tracker in the hierarchy. Does not change after initialization.
  ReservationTracker* parent_ = nullptr;

  /// If non-NULL, reservations are counted as memory consumption against this tracker.
  /// Does not change after initialization. Not owned.
  /// TODO: remove once all memory is accounted via ReservationTrackers.
  MemTracker* mem_tracker_ = nullptr;

  /// Determines whether the soft or hard limit of 'mem_tracker_' is checked for
  /// reservation increases.
  MemLimit mem_limit_mode_;

  /// The maximum reservation in bytes that this tracker can have. Can be read without
  /// holding lock.
  AtomicInt64 reservation_limit_;

  /// This tracker's current reservation in bytes. 'reservation_' <= 'reservation_limit_'.
  /// Can be read without holding lock.
  AtomicInt64 reservation_;

  /// Total reservation of children in bytes. This is included in 'reservation_'.
  /// 'used_reservation_' + 'child_reservations_' <= 'reservation_'. Can be read without
  /// holding lock.
  AtomicInt64 child_reservations_;

  /// The amount of the reservation currently used by this tracker in bytes.
  /// 'used_reservation_' + 'child_reservations_' <= 'reservation_'.
  /// Can be read without holding lock.
  AtomicInt64 used_reservation_;
};
}

#endif
