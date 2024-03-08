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

#include "util/cache/cache.h"
#include "util/cache/cache-internal.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/atomic/atomic.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/status.h"
#include "gutil/mathlimits.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/alignment.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

DECLARE_double(cache_memtracker_approximation_ratio);
DEFINE_double_hidden(lirs_unprotected_percentage, 5.00,
    "Percentage of the LIRS cache used for unprotected entries. This must be between "
    "0.0 and 50.0.");
DEFINE_double_hidden(lirs_tombstone_multiple, 2.00,
    "Multiple of tombstone entries to resident entries for LIRS cache. "
    "If there are 100 normal entries, a multiple of 2.0 would allow 200 tombstones. "
    "This is required to be at least 0.50.");

using std::string;
using std::vector;

using boost::intrusive::member_hook;
using boost::intrusive::list_member_hook;

using kudu::Slice;

using strings::Substitute;

namespace impala {

namespace {

typedef kudu::simple_spinlock MutexType;

// This implements the Low Inter-reference Recency Set (LIRS) algorithm described in
// "LIRS: An Efficient Low Inter-reference Recency Set Replacement Policy to Improve
//  Buffer Cache Performance" Song Jiang and Xiaodong Zhang 2002.
//
// LIRS is based on a combination of recency and reuse distance. Recency is the number
// of other accesses since the last access to a particular key. Reuse distance is
// the number of accesses between the last access and the second to last access to a
// particular key. i.e. If we are now at access a2 and the last two access are at a0
// and a1, the reuse distance is a1-a0 and the recency is a2-a1:
// a0------ reuse distance ---------a1-------------- recency ---------------- a2 (now)
// If the key has only been accessed once, its reuse distance is considered infinite.
// Recency provides information about what the next reuse distance will be.
// The intuition is that entries with a shorter reuse distance are more likely to be
// revisited than ones with longer reuse distance.
//
// LIRS partitions the cache between two categories: LIR (low interreference recency)
// and HIR (high interreference recency), and it has three different types of entries:
// LIR, HIR resident, HIR nonresident. For clarity, these are renamed to be
// PROTECTED (LIR), UNPROTECTED (HIR resident), and TOMBSTONE (HIR nonresident).
// PROTECTED entries have been seen more than once recently and are "protected" from
// eviction. A PROTECTED entry is only evicted after it transitions to being UNPROTECTED.
// UNPROTECTED entries are new or infrequently accessed and can be evicted directly.
// PROTECTED and UNPROTECTED entries occupy space in the cache and are accessible via
// Lookup(). TOMBSTONE entries are metadata-only entries that have already been evicted.
// They are not accessible via Lookup() and they don't occupy space in cache.
// The purpose of UNPROTECTED and TOMBSTONE entries are to provide a way to identify when
// there are new or previously infrequent entries that now have shorter interreference
// recency than existing PROTECTED entries. If an entry in the UNPROTECTED or TOMBSTONE
// categories is seen again and has a lower interreference recency than the oldest
// PROTECTED entry, the UNPROTECTED/TOMBSTONE entry is promoted to PROTECTED and the
// oldest PROTECTED entry is demoted to UNPROTECTED. Entries that are not in the cache,
// either because they were never added or have been completely removed, have the
// UNINITIALIZED state. Every handle starts in UNINITIALIZED and will transition to
// UNINITIALIZED before being freed.
//
// The cache capacity is split between the UNPROTECTED and PROTECTED categories. The size
// of the UNPROTECTED category is determined by the lirs_unprotected_percentage
// parameter, which is typically in the 1-5% range. All remaining space is used for
// PROTECTED entries.
//
// The implementation uses two linked lists:
// 1. The recency list contains all types of entries in the order in which they were last
//    seen. The recency list maintains the invariant that the oldest entry is a PROTECTED
//    entry. If a modification to the recency list results in the oldest entry (or
//    entries) being UNPROTECTED or TOMBSTONE entries, they are removed. This guarantees
//    that if an UNPROTECTED or TOMBSTONE entry is seen again while on the recency list,
//    it has a reuse distance shorter than the oldest entry on the recency list, so it
//    can be upgraded to a PROTECTED entry (potentially evicting the oldest PROTECTED
//    entry).
// 2. The unprotected list contains only UNPROTECTED entries. Something can enter the
//    unprotected list by being a previously unseen entry, or it can enter the
//    unprotected list by being demoted from the PROTECTED state. The UNPROTECTED entries
//    must fit within a fixed percentage of the cache, so when an entry is added to the
//    unprotected list, other entries may be removed. Entries removed from the unprotected
//    list are evicted from the cache and become TOMBSTONE entries.

enum LIRSState : uint8_t {
  UNINITIALIZED = 0,
  PROTECTED = 1,
  UNPROTECTED = 2,
  TOMBSTONE = 3
};

// LIRS has PROTECTED/UNPROTECTED entries that are resident (i.e. occupying cache
// capacity) as well as TOMBSTONE entries that are not resident (i.e. do not occupy cache
// capacity). DataResidency tracks whether an entry is cache resident or not. Entries
// always start and end as NOT_RESIDENT. Insert() transitions an entry from NOT_RESIDENT
// to RESIDENT. When an entry is evicted, it transitions to EVICTING, runs the eviction
// callback, then transitions to NOT_RESIDENT.
//
// This is distinct from the LIRSState, because eviction can only happen if there are no
// external references. A TOMBSTONE entry (which should be evicted) can still be RESIDENT
// if there is an external reference that has not been released. See the description of
// AtomicState for how DataResidency is used to synchronize eviction between threads.
enum DataResidency : uint8_t {
  NOT_RESIDENT = 0,
  RESIDENT = 1,
  EVICTING = 2,
};

// The AtomicState contains all the state for an entry interacting with the LIRS
// cache. A large number of the transitions are operating on a single field in the
// struct (incrementing the refererence count, changing the state, etc), and they occur
// while holding the cache-wide mutex. For example, the LIRSState transitions always
// hold the mutex, and ref_count is only incremented while holding the mutex. This is
// an atomic struct to allow some codepaths such as Release() and CleanupThreadState()
// to avoid getting a mutex. Residency can be manipulated and ref_count can be
// decremented without holding a mutex.
//
// All of the interesting state transitions involve reaching agreement about which thread
// should evict and/or free an entry.
//
// Due to the existence of TOMBSTONE entries, eviction and free are now separate
// actions. For eviction, every codepath that is modifying the atomic state to an
// evictable state must determine if it should perform the eviction. The criteria
// for eviction is that state = TOMBSTONE or state = UNINITIALIZED and ref_count = 0 and
// residency = RESIDENT. If a thread is going to reach this state, it must instead set
// residency = EVICTING and perform the eviction. Setting residency = EVICTING makes
// it impossible for a different thread to reach an evictable state, so eviction will
// happen exactly once.
//
// Determining which thread frees an entry is determined by which thread reaches
// state = UNINITIALIZED, ref_count = 0, residency = NOT_RESIDENT. The scenario that this
// is designed to solve is this:
// Entry starts with state = TOMBSTONE, ref_count = 1, resident = RESIDENT
// Thread 1: calls Release(), decrementing ref_count to 0 and setting resident = EVICTING
// Thread 1: starts running CleanupThreadState()
// Thread 2: starts running ToUninitialized()
// There is a race between Thread 1 and Thread 2.
// If Thread 1 sets resident = NOT_RESIDENT first, Thread 2 performs free.
// If Thread 2 sets state = UNINITIALIZED first, Thread 1 performs free.
// Eviction is not assumed to be fast or instantaneous, so it is useful that Thread 2
// does not need to wait for Thread 1 to complete eviction.
//
// This is 32-bits to allow simple atomic operations and so that AtomicStateTransition is
// 64-bits.
struct AtomicState {
  // 'ref_count' is a count of the number of external references (i.e. handles that
  // have been returned from Lookup() or Insert() but have not yet called Release()).
  // This is not specific to LIRS, every cache algorithm needs an equivalent count.
  // This is strictly external references. LIRS does not count itself as a reference,
  // because it has a separate state field to track this. So, if there are no external
  // references, this is zero. In order for an entry to be evicted or freed, the
  // ref_count must be zero.
  uint16_t ref_count;
  LIRSState state;
  DataResidency residency;
};
static_assert(sizeof(AtomicState) == 4, "AtomicState has unexpected size");

// Struct to represent a successful atomic transition with the before and after values
struct AtomicStateTransition {
  AtomicStateTransition(AtomicState x, AtomicState y)
    : before(x), after(y) {}
  AtomicState before;
  AtomicState after;
};
static_assert(sizeof(AtomicStateTransition) == 8,
              "AtomicStateTransition has unexpected size");

class LIRSHandle : public HandleBase {
public:
  LIRSHandle(uint8_t* kv_ptr, const Slice& key, int32_t hash, int val_len, int charge)
    : HandleBase(kv_ptr, key, hash, val_len, charge) {
    AtomicState init_state;
    init_state.ref_count = 0;
    init_state.state = UNINITIALIZED;
    init_state.residency = NOT_RESIDENT;
    atomic_state_.store(init_state);
  }

  ~LIRSHandle() {
    AtomicState destruct_state = atomic_state_.load();
    DCHECK_EQ(destruct_state.state, UNINITIALIZED);
    DCHECK_EQ(destruct_state.ref_count, 0);
    DCHECK_EQ(destruct_state.residency, NOT_RESIDENT);
  }

  Cache::EvictionCallback* eviction_callback_;
  // Recency list double-link
  list_member_hook<> recency_list_hook_;
  // Unprotected/Tombstone list double-link.
  list_member_hook<> unprotected_tombstone_list_hook_;

  // The boost functionality is the same as the std library functionality. Using boost
  // solves a linking issue when using UBSAN with dynamic linking.
  // TODO: Switch back to std library atomics when this works.
  boost::atomic<AtomicState> atomic_state_;

  // Since compare and swap can fail when there are concurrent modifications, this
  // wraps some of the boilerplate compare and swap retry logic. This performs
  // the provided lambda in a loop until the compare and swap succeeds.
  // The lambda is required to mutate the AtomicState argument passed to it.
  // This returns a structure containing the old atomic value and new atomic value.
  AtomicStateTransition modify_atomic_state(std::function<void(AtomicState&)> fn) {
    AtomicState old_atomic_state = atomic_state_.load();
    AtomicState new_atomic_state = old_atomic_state;
    while (true) {
      new_atomic_state = old_atomic_state;
      // fn mutates new_atomic_state and must change something
      fn(new_atomic_state);
      DCHECK(old_atomic_state.ref_count != new_atomic_state.ref_count ||
             old_atomic_state.state != new_atomic_state.state ||
             old_atomic_state.residency != new_atomic_state.residency);
      if (atomic_state_.compare_exchange_weak(old_atomic_state, new_atomic_state)) {
        break;
      }
    }
    return AtomicStateTransition(old_atomic_state, new_atomic_state);
  }

  LIRSState state() const {
    return atomic_state_.load().state;
  }

  // The caller must be holding the mutex_.
  void set_state(LIRSState old_state, LIRSState new_state) {
    modify_atomic_state([old_state, new_state](AtomicState& cur_state) {
        DCHECK_EQ(cur_state.state, old_state);
        cur_state.state = new_state;
      });
  }

  DataResidency residency() const {
    return atomic_state_.load().residency;
  }

  void set_resident() {
    modify_atomic_state([](AtomicState& cur_state) {
        DCHECK_EQ(cur_state.residency, NOT_RESIDENT);
        cur_state.residency = RESIDENT;
      });
  }

  uint16_t num_references() const {
    return atomic_state_.load().ref_count;
  }

  // The caller must be holding the mutex_.
  void get_reference() {
    modify_atomic_state([](AtomicState& cur_state) {
        // Check whether the reference count would wrap. There is no use case that
        // currently needs to exceed 65k concurrent references, and this is likely
        // to be a bug in the caller's code. For now, this asserts, but there may be
        // a better behavior if needed.
        CHECK_LT(cur_state.ref_count, std::numeric_limits<uint16_t>::max());
        ++cur_state.ref_count;
      });
  }
};

struct LIRSThreadState {
  // Handles to evict (must already be in TOMBSTONE or UNINITIALIZED state), because
  // eviction is done without holding a mutex.
  vector<LIRSHandle*> handles_to_evict;
  // Handles to free (must already be in the UNINITIALIZED state and removed from
  // structures).
  vector<LIRSHandle*> handles_to_free;
};

// Boost intrusive lists allow the handle to easily be on multiple lists simultaneously.
// Each hook is equivalent to a prev/next pointer.
typedef member_hook<LIRSHandle, list_member_hook<>,
    &LIRSHandle::recency_list_hook_> RecencyListOption;
typedef boost::intrusive::list<LIRSHandle, RecencyListOption> RecencyList;

typedef member_hook<LIRSHandle, list_member_hook<>,
    &LIRSHandle::unprotected_tombstone_list_hook_> UnprotectedTombstoneListOption;
typedef boost::intrusive::list<LIRSHandle,
    UnprotectedTombstoneListOption> UnprotectedTombstoneList;

// A single shard of sharded cache.
class LIRSCacheShard : public CacheShard {
 public:
  explicit LIRSCacheShard(kudu::MemTracker* tracker, size_t capacity);
  ~LIRSCacheShard();

  Status Init() override;
  HandleBase* Allocate(Slice key, uint32_t hash, int val_len, size_t charge) override;
  void Free(HandleBase* handle) override;
  HandleBase* Insert(HandleBase* handle,
      Cache::EvictionCallback* eviction_callback) override;
  HandleBase* Lookup(const Slice& key, uint32_t hash, bool no_updates) override;
  void UpdateCharge(HandleBase* handle, size_t charge) override;
  void Release(HandleBase* handle) override;
  void Erase(const Slice& key, uint32_t hash) override;
  size_t Invalidate(const Cache::InvalidationControl& ctl) override;
  vector<HandleBase*> Dump() override;

 private:

  // Update the memtracker's consumption by the given amount.
  //
  // This "buffers" the updates locally in 'deferred_consumption_' until the amount
  // of accumulated delta is more than ~1% of the cache capacity. This improves
  // performance under workloads with high eviction rates for a few reasons:
  //
  // 1) once the cache reaches its full capacity, we expect it to remain there
  // in steady state. Each insertion is usually matched by an eviction, and unless
  // the total size of the evicted item(s) is much different than the size of the
  // inserted item, each eviction event is unlikely to change the total cache usage
  // much. So, we expect that the accumulated error will mostly remain around 0
  // and we can avoid propagating changes to the MemTracker at all.
  //
  // 2) because the cache implementation is sharded, we do this tracking in a bunch
  // of different locations, avoiding bouncing cache-lines between cores. By contrast
  // the MemTracker is a simple integer, so it doesn't scale as well under concurrency.
  //
  // Positive delta indicates an increased memory consumption.
  void UpdateMemTracker(int64_t delta);

  // MoveToRecencyListBack takes an entry and makes it the most recent entry in the
  // recency list.
  void MoveToRecencyListBack(LIRSThreadState* tstate, LIRSHandle* e,
      bool expected_in_list=true);

  // TrimRecencyList maintains the invariant that the oldest element in the recency list
  // is a protected entry. This iterates the recency list from oldest to newest, removing
  // entries until the oldest entry is a PROTECTED entry.
  void TrimRecencyList(LIRSThreadState* tstate);

  // Helper functions for adding to the UNPROTECTED block in the
  // unprotected_tombstone_list_. This sets unprotected_list_front_ if this
  // is the first UNPROTECTED entry in the list. This function relies on
  // appropriate ordering of operations. List operations like this need to
  // happen before updating counts/usage (e.g. num_unprotected_).
  void AddToUnprotectedList(LIRSHandle* e);

  // Helper function for removing from the UNPROTECTED block in the
  // unprotected_tombstone_list_. This detects whether this is the front of the
  // unprotected block and maintains unprotected_list_front_ appropriately.
  // This function relies on appropriate ordering of operations. List operations
  // like this need to happen before updating counts/usage (e.g. num_unprotected_).
  void RemoveFromUnprotectedList(LIRSHandle* e);

  // This iterates the recency list from oldest to newest removing TOMBSTONE entires
  // until the number of TOMBSTONE entries is less than the limit specified by
  // lirs_tombstone_multiple.
  void EnforceTombstoneLimit(LIRSThreadState* tstate);

  // This enforces the capacity constraint on the PROTECTED entries. As long as the
  // protected usage exceeds the protected capacity, it removes the oldest PROTECTED
  // entry from the recency list and transitions it to an UNPROTECTED entry.
  void EnforceProtectedCapacity(LIRSThreadState* tstate);

  // This enforces the capacity constraint on the UNPROTECTED entries. As long as the
  // unprotected usage exceeds the unprotected capacity, it removes the oldest
  // UNPROTECTED entry from the unprotected list. The removed entry becomes
  // a TOMBSTONE entry or an UNINITIALIZED entry.
  void EnforceUnprotectedCapacity(LIRSThreadState* tstate);

  // Enter the cache
  // UninitializedToProtected always succeeds
  void UninitializedToProtected(LIRSThreadState* tstate, LIRSHandle* e);
  // UninitializedToUnprotected can fail when the entry is too large. Returns whether the
  // entry was succcessfully added.
  bool UninitializedToUnprotected(LIRSThreadState* tstate, LIRSHandle* e);

  // Bounce around the cache
  void UnprotectedToProtected(LIRSThreadState* tstate, LIRSHandle* e);
  void ProtectedToUnprotected(LIRSThreadState* tstate, LIRSHandle* e);
  void UnprotectedToTombstone(LIRSThreadState* tstate, LIRSHandle* e);

  // Exit the cache
  void ToUninitialized(LIRSThreadState* tstate, LIRSHandle* e, bool is_trim = false);

  // Functions move evictions and frees outside the critical section for mutex_ by
  // placing the affected entries on the LIRSThreadState. This function handles the
  // accumulated evictions/frees. It should be called without holding any locks.
  void CleanupThreadState(LIRSThreadState* tstate);

  bool initialized_ = false;

  // Capacity is split between protected and unprotected entities. Unprotected entities
  // make up a much smaller proportion of the cache than protected.
  // Note: updates to counts/usage happen after doing list operations.
  size_t capacity_;
  size_t unprotected_capacity_;
  size_t unprotected_usage_ = 0;
  size_t num_unprotected_ = 0;
  size_t protected_capacity_;
  size_t protected_usage_ = 0;
  size_t num_protected_ = 0;
  size_t num_tombstones_ = 0;

  // mutex_ protects the following state.
  MutexType mutex_;

  // Recency list (called the LIRS stack in the paper)
  RecencyList recency_list_;

  // This list is a combination of the unprotected list (called the HIR list in the
  // paper) and the tombstone list (not in the paper). The tombstone list is used
  // to efficiently implement the lirs_tombstone_multiple. Without a tombstone list,
  // finding the oldest TOMBSTONE entry requires traversing the recency list,
  // potentially passing a large number of entries before reaching a TOMBSTONE entry.
  // The tombstone list makes this O(1).
  //
  // The combined list still behaves like two separate lists. In particular, the
  // UNPROTECTED entries and TOMBSTONE entries are each contiguous blocks.
  // The block of newer UNPROTECTED entries is at the back of the list. The block
  // of oldest TOMBSTONE entries is at the front. The unprotected_list_front_ points
  // to the UNPROTECTED entry that is at the block boundary.
  // Visually, it is:
  // newest/end() |--- UNPROTECTED entries ---|--- TOMBSTONE entries ---| oldest/begin()
  //               ^                         ^                         ^
  //              back()             unprotected_list_front_          front()
  //
  // Combining the two lists reduces the cost of transferring an element from
  // UNPROTECTED to TOMBSTONE, which is a common operation. To simplify operations on the
  // UNPROTECTED block, see the two helper functions AddToUnprotectedList() and
  // RemoveFromUnprotectedList().
  UnprotectedTombstoneList unprotected_tombstone_list_;

  // The front of the UNPROTECTED block in the unprotected_tombstone_list_. This is
  // nullptr if there are no UNPROTECTED entries.
  LIRSHandle* unprotected_list_front_ = nullptr;

  // Hash table that maps keys to handles.
  HandleTable table_;

  kudu::MemTracker* mem_tracker_;
  std::atomic<int64_t> deferred_consumption_ { 0 };

  // Initialized based on capacity_ to ensure an upper bound on the error on the
  // MemTracker consumption.
  int64_t max_deferred_consumption_;
};

LIRSCacheShard::LIRSCacheShard(kudu::MemTracker* tracker, size_t capacity)
  : capacity_(capacity), mem_tracker_(tracker) {}

LIRSCacheShard::~LIRSCacheShard() {
  LIRSThreadState tstate;
  // Start with the recency list.
  while (!recency_list_.empty()) {
    LIRSHandle* e = &recency_list_.front();
    DCHECK_NE(e->state(), UNINITIALIZED);
    DCHECK_EQ(e->num_references(), 0);
    // This removes it from the recency list and the unprotected list (if appropriate)
    ToUninitialized(&tstate, e);
  }
  // Anything remaining was not on the recency list. All the TOMBSTONE entries are
  // on the recency list and get removed in the previous step, so this is now purely
  // UNPROTECTED entries.
  while (!unprotected_tombstone_list_.empty()) {
    LIRSHandle* e = &*unprotected_tombstone_list_.begin();
    DCHECK_EQ(e->state(), UNPROTECTED);
    DCHECK_EQ(e->num_references(), 0);
    // This removes it from the unprotected list
    ToUninitialized(&tstate, e);
  }
  DCHECK(unprotected_list_front_ == nullptr);
  CleanupThreadState(&tstate);
  // Eviction calls UpdateMemTracker() for each handle, but UpdateMemTracker() only
  // updates the underlying mem_tracker_ if the deferred consumption exceeds the
  // max deferred consumption. There can be deferred consumption left over here,
  // so decrement any remaining deferred consumption.
  mem_tracker_->Consume(deferred_consumption_);
}

Status LIRSCacheShard::Init() {
  // To save some implementation complexity, avoid dealing with unprotected
  // percentage >= 50.00%. This is far outside the range of what is useful for
  // LIRS.
  if (!MathLimits<double>::IsFinite(FLAGS_lirs_unprotected_percentage) ||
      FLAGS_lirs_unprotected_percentage >= 50.0 ||
      FLAGS_lirs_unprotected_percentage < 0.0) {
    return Status(Substitute("Misconfigured --lirs_unprotected_percentage: $0. "
        "Must be between 0 and 50.", FLAGS_lirs_unprotected_percentage));
  }
  if (!MathLimits<double>::IsFinite(FLAGS_cache_memtracker_approximation_ratio) ||
      FLAGS_cache_memtracker_approximation_ratio < 0.0 ||
      FLAGS_cache_memtracker_approximation_ratio > 1.0) {
    return Status(Substitute("Misconfigured --cache_memtracker_approximation_ratio: $0. "
        "Must be between 0 and 1.", FLAGS_cache_memtracker_approximation_ratio));
  }
  if (!MathLimits<double>::IsFinite(FLAGS_lirs_tombstone_multiple) ||
      FLAGS_lirs_tombstone_multiple < 0.5) {
    return Status(Substitute("Misconfigured --lirs_tombstone_multiple: $0. "
        "Must be 0.5 or greater.", FLAGS_lirs_tombstone_multiple));
  }
  unprotected_capacity_ =
      static_cast<size_t>((capacity_ * FLAGS_lirs_unprotected_percentage) / 100.00);
  protected_capacity_ = capacity_ - unprotected_capacity_;
  CHECK_GE(protected_capacity_, unprotected_capacity_);
  max_deferred_consumption_ = capacity_ * FLAGS_cache_memtracker_approximation_ratio;
  initialized_ = true;
  return Status::OK();
}

void LIRSCacheShard::UpdateMemTracker(int64_t delta) {
  int64_t old_deferred = deferred_consumption_.fetch_add(delta);
  int64_t new_deferred = old_deferred + delta;

  if (new_deferred > max_deferred_consumption_ ||
      new_deferred < -max_deferred_consumption_) {
    int64_t to_propagate = deferred_consumption_.exchange(0, std::memory_order_relaxed);
    mem_tracker_->Consume(to_propagate);
  }
}

void LIRSCacheShard::EnforceProtectedCapacity(LIRSThreadState* tstate) {
  while (protected_usage_ > protected_capacity_) {
    DCHECK(!recency_list_.empty());
    // Get pointer to oldest entry and remove it
    LIRSHandle* oldest = &recency_list_.front();
    // The oldest entry must be protected (i.e. the recency list must be trimmed)
    DCHECK_EQ(oldest->state(), PROTECTED);
    ProtectedToUnprotected(tstate, oldest);
  }
}

void LIRSCacheShard::TrimRecencyList(LIRSThreadState* tstate) {
  // This function maintains the invariant that the oldest entry in the list must be
  // a protected entry. Look at the oldest entry in the list (i.e. the front). If it is
  // not protected, remove it from the list. If it is a tombstone entry, it needs to be
  // deleted. Unprotected entries still exist in the unprotected list, so UNPROTECTED
  // entries should only be removed from the recency list.
  while (!recency_list_.empty() && recency_list_.front().state() != PROTECTED) {
    LIRSHandle* oldest = &recency_list_.front();
    if (oldest->state() == TOMBSTONE) {
      ToUninitialized(tstate, oldest, /* is_trim */ true);
    } else {
      DCHECK_EQ(oldest->state(), UNPROTECTED);
      recency_list_.pop_front();
    }
  }
}

void LIRSCacheShard::EnforceTombstoneLimit(LIRSThreadState* tstate) {
  // If there are a large number of entries that haven't been seen before, the number
  // of tombstones can grow without bound. This enforces an upper bound on the total
  // number of tombstones to limit the metadata size. This is defined as a multiple of
  // the total number of entries in the cache.
  // TODO: It would help performance to enforce this with some inexactness (i.e. batch
  // the removals).
  size_t num_elems = num_unprotected_ + num_protected_;
  size_t tombstone_limit =
    static_cast<size_t>(static_cast<double>(num_elems) * FLAGS_lirs_tombstone_multiple);
  if (num_tombstones_ <= tombstone_limit) return;
  // Remove the oldest TOMBSTONE entries from the front of unprotected_tombstone_list_
  // until we are back under the limit.
  auto it = unprotected_tombstone_list_.begin();
  while (num_tombstones_ > tombstone_limit) {
    LIRSHandle* e = &*it;
    it = unprotected_tombstone_list_.erase(it);
    DCHECK_EQ(e->state(), TOMBSTONE);
    // This will remove the entry from the recency_list_.
    ToUninitialized(tstate, e);
  }
}

void LIRSCacheShard::EnforceUnprotectedCapacity(LIRSThreadState* tstate) {
  while (unprotected_usage_ > unprotected_capacity_) {
    // Evict the oldest UNPROTECTED entry from the unprotected list. This transitions it
    // from UNPROTECTED to a TOMBSTONE entry
    DCHECK_GT(num_unprotected_, 0);
    DCHECK(!unprotected_tombstone_list_.empty());
    DCHECK(unprotected_list_front_ != nullptr);
    LIRSHandle* oldest = unprotected_list_front_;
    if (oldest->recency_list_hook_.is_linked()) {
      UnprotectedToTombstone(tstate, oldest);
    } else {
      // If the oldest entry is not on the recency list, then its reuse distance is
      // longer than the oldest entry on the recency list. This entry should be deleted
      // rather than becoming a TOMBSTONE.
      ToUninitialized(tstate, oldest);
    }
  }
}

void LIRSCacheShard::MoveToRecencyListBack(LIRSThreadState* tstate, LIRSHandle *e,
    bool expected_in_list) {
  // Remove and add to the back of the list as the most recent element
  bool in_list = e->recency_list_hook_.is_linked();
  CHECK(!expected_in_list || in_list);
  bool need_trim = false;
  if (in_list) {
    // Is this the oldest entry in the list (the front)?
    LIRSHandle* oldest = &recency_list_.front();
    // Invariant: the oldest entry in the list is always a protected entry
    CHECK_EQ(oldest->state(), PROTECTED);
    if (oldest == e) {
      need_trim = true;
    }
    recency_list_.erase(recency_list_.iterator_to(*e));
  }
  recency_list_.push_back(*e);
  if (need_trim) {
    TrimRecencyList(tstate);
  }
}

void LIRSCacheShard::AddToUnprotectedList(LIRSHandle* e) {
  DCHECK(!e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK_EQ(e->state(), UNPROTECTED);
  unprotected_tombstone_list_.push_back(*e);
  // Updates to counts are always after list operations. If the list is empty,
  // this count is zero.
  if (num_unprotected_ == 0) {
    DCHECK(unprotected_list_front_ == nullptr);
    unprotected_list_front_ = e;
  }
}

void LIRSCacheShard::RemoveFromUnprotectedList(LIRSHandle* e) {
  DCHECK(e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK_EQ(e->state(), UNPROTECTED);
  DCHECK_GE(num_unprotected_, 1);
  auto it = unprotected_tombstone_list_.iterator_to(*e);
  it = unprotected_tombstone_list_.erase(it);
  // If it was the front of the list, then unprotected_list_front_ needs to be
  // updated.
  if (e == unprotected_list_front_) {
    // Updates to counts are always after list operations, so num_unprotected_
    // includes this entry.
    if (num_unprotected_ > 1) {
      // This was not the last unprotected entry
      DCHECK(it != unprotected_tombstone_list_.end());
      unprotected_list_front_ = &*it;
    } else {
      // This was the last unprotected entry
      DCHECK(it == unprotected_tombstone_list_.end());
      unprotected_list_front_ = nullptr;
    }
  }
}

void LIRSCacheShard::UnprotectedToProtected(LIRSThreadState* tstate, LIRSHandle *e) {
  // This only happens for entries that are only the recency list
  DCHECK(e->recency_list_hook_.is_linked());
  // Make this the most recent entry in the recency list and remove from the
  // unprotected list.
  MoveToRecencyListBack(tstate, e);
  RemoveFromUnprotectedList(e);
  e->set_state(UNPROTECTED, PROTECTED);
  // List operations happen before changes to the counts/usage
  --num_unprotected_;
  ++num_protected_;
  // Transfer the usage
  unprotected_usage_ -= e->charge();
  protected_usage_ += e->charge();
  // If necessary, evict old entries from the recency list, protected to unprotected
  EnforceProtectedCapacity(tstate);
}

void LIRSCacheShard::ProtectedToUnprotected(LIRSThreadState* tstate, LIRSHandle* e) {
  DCHECK(!e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK(e->recency_list_hook_.is_linked());
  // Must be the oldest on the recency list
  DCHECK(&recency_list_.front() == e);
  recency_list_.erase(recency_list_.iterator_to(*e));
  // Trim the recency list after the removal
  TrimRecencyList(tstate);
  e->set_state(PROTECTED, UNPROTECTED);
  // Add to unprotected list as the newest entry
  AddToUnprotectedList(e);
  // List operations happen before changes to the counts/usage
  --num_protected_;
  ++num_unprotected_;
  // Transfer the usage
  protected_usage_ -= e->charge();
  unprotected_usage_ += e->charge();
  // If necessary, evict an entry from the unprotected list
  EnforceUnprotectedCapacity(tstate);
}

void LIRSCacheShard::UnprotectedToTombstone(LIRSThreadState* tstate, LIRSHandle* e) {
  // Decrement unprotected usage
  DCHECK(e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK(e->recency_list_hook_.is_linked());

  // Optimized code to go from UNPROTECTED to TOMBSTONE without adding/removing
  // it from a list. Only the unprotected_list_front_ moves.
  DCHECK_EQ(e, unprotected_list_front_);
  if (num_unprotected_ > 1) {
    // There is some other UNPROTECTED entry
    auto it = unprotected_tombstone_list_.iterator_to(*e);
    ++it;
    unprotected_list_front_ = &*it;
  } else {
    // This was the only UNPROTECTED entry, the unprotected list is empty
    unprotected_list_front_ = nullptr;
  }

  // Set state to TOMBSTONE. If there are currently no references, this thread is
  // responsible for eviction, so it must also set the residency to EVICTING.
  auto state_transition = e->modify_atomic_state([](AtomicState& cur_state) {
      DCHECK_EQ(cur_state.state, UNPROTECTED);
      DCHECK_EQ(cur_state.residency, RESIDENT);
      cur_state.state = TOMBSTONE;
      if (cur_state.ref_count == 0) cur_state.residency = EVICTING;
    });
  // Look at the old state. If there are no references, then we can immediately evict.
  if (state_transition.before.ref_count == 0) {
    tstate->handles_to_evict.push_back(e);
  }
  // List operations happen before changes to the counts/usage
  --num_unprotected_;
  ++num_tombstones_;

  // This will be evicted, so the charge disappears
  unprotected_usage_ -= e->charge();
  EnforceTombstoneLimit(tstate);
}

void LIRSCacheShard::UninitializedToProtected(LIRSThreadState* tstate, LIRSHandle* e) {
  DCHECK(!e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK(!e->recency_list_hook_.is_linked());
  e->set_state(UNINITIALIZED, PROTECTED);
  HandleBase* existing = table_.Insert(e);
  DCHECK(existing == nullptr);
  recency_list_.push_back(*e);
  // List operations happen before changes to the counts/usage
  ++num_protected_;
  protected_usage_ += e->charge();
  EnforceProtectedCapacity(tstate);
}

bool LIRSCacheShard::UninitializedToUnprotected(LIRSThreadState* tstate, LIRSHandle* e) {
  DCHECK(!e->unprotected_tombstone_list_hook_.is_linked());
  DCHECK(!e->recency_list_hook_.is_linked());
  e->set_state(UNINITIALIZED, UNPROTECTED);
  HandleBase* existing = table_.Insert(e);
  DCHECK(existing == nullptr);
  recency_list_.push_back(*e);
  AddToUnprotectedList(e);
  // List operations happen before changes to the counts/usage
  ++num_unprotected_;
  unprotected_usage_ += e->charge();
  // If necessary, evict an entry from the unprotected list
  EnforceUnprotectedCapacity(tstate);
  // There is exactly one failure case:
  // If the new entry is larger than the unprotected capacity, then it will cause all
  // unprotected entries to be removed (including itself).
  if (e->charge() > unprotected_capacity_) {
    DCHECK(e->state() == UNINITIALIZED || e->state() == TOMBSTONE);
    DCHECK_EQ(num_unprotected_, 0);
    return false;
  }
  // The entry remains in the cache
  DCHECK_EQ(e->state(), UNPROTECTED);
  DCHECK(!unprotected_tombstone_list_.empty());
  DCHECK(e->unprotected_tombstone_list_hook_.is_linked());
  return true;
}

void LIRSCacheShard::ToUninitialized(LIRSThreadState* tstate, LIRSHandle* e,
    bool is_trim) {
  DCHECK_NE(e->state(), UNINITIALIZED);
  LIRSHandle* removed_elem = static_cast<LIRSHandle*>(table_.Remove(e->key(), e->hash()));
  DCHECK(e == removed_elem || removed_elem == nullptr);
  // Remove from the list (if it is in the list)
  if (e->recency_list_hook_.is_linked()) {
    // If we are removing the last entry on the recency list, then we may need to call
    // TrimRecencyList to maintain our invariant that the last entry on the recency list
    // is a PROTECTED element. TrimRecencyList itself calls this function while enforcing
    // the invariant, so this passes is_trim=true from TrimRecencyList to avoid the
    // unnecessary recursion.
    bool need_trim = false;
    // Is this the oldest entry in the list (the front)?
    LIRSHandle* oldest = &recency_list_.front();
    if (!is_trim and oldest == e) {
      DCHECK_EQ(e->state(), PROTECTED);
      need_trim = true;
    }
    recency_list_.erase(recency_list_.iterator_to(*e));
    if (need_trim) {
      TrimRecencyList(tstate);
    }
  }
  if (e->state() == UNPROTECTED) {
    if (e->unprotected_tombstone_list_hook_.is_linked()) {
      // Remove from the unprotected list and decrement usage
      RemoveFromUnprotectedList(e);
    }
    --num_unprotected_;
    unprotected_usage_ -= e->charge();
  } else if (e->state() == TOMBSTONE) {
    if (e->unprotected_tombstone_list_hook_.is_linked()) {
      unprotected_tombstone_list_.erase(unprotected_tombstone_list_.iterator_to(*e));
    }
    --num_tombstones_;
  } else {
    DCHECK(!e->unprotected_tombstone_list_hook_.is_linked());
  }
  if (e->state() == PROTECTED) {
    protected_usage_ -= e->charge();
    --num_protected_;
  }

  auto state_transition = e->modify_atomic_state([](AtomicState& cur_state) {
      cur_state.state = UNINITIALIZED;
      // If it is not resident, there must be no references.
      if (cur_state.residency == NOT_RESIDENT || cur_state.residency == EVICTING) {
        DCHECK_EQ(cur_state.ref_count, 0);
      }
      // If this is resident without references, we can start eviction.
      if (cur_state.residency == RESIDENT && cur_state.ref_count == 0) {
        cur_state.residency = EVICTING;
      }
    });

  if (state_transition.before.residency == RESIDENT &&
      state_transition.after.residency == EVICTING) {
    // We did a transition from RESIDENT to EVICTING, so we are responsible for
    // eviction. Since we know the new state is UNINITIALIZED, we are also responsible
    // for freeing it, but the eviction code will handle that appropriately.
    tstate->handles_to_evict.push_back(e);
  }

  // This was already evicted, so now it just needs to be freed.
  if (state_transition.before.residency == NOT_RESIDENT) {
    tstate->handles_to_free.push_back(e);
  }
}

HandleBase* LIRSCacheShard::Allocate(Slice key, uint32_t hash, int val_len,
    size_t charge) {
  DCHECK(initialized_);
  int key_len = key.size();
  DCHECK_GE(key_len, 0);
  DCHECK_GE(val_len, 0);
  DCHECK_GT(charge, 0);
  if (charge == 0 || charge > protected_capacity_) {
    return nullptr;
  }
  int key_len_padded = KUDU_ALIGN_UP(key_len, sizeof(void*));
  uint8_t* buf = new uint8_t[sizeof(LIRSHandle)
                             + key_len_padded + val_len]; // the kv_data VLA data
  int calc_charge =
    (charge == Cache::kAutomaticCharge) ? kudu::kudu_malloc_usable_size(buf) : charge;
  uint8_t* kv_ptr = buf + sizeof(LIRSHandle);
  LIRSHandle* handle = new (buf) LIRSHandle(kv_ptr, key, hash, val_len,
      calc_charge);
  return handle;
}

void LIRSCacheShard::Free(HandleBase* handle) {
  DCHECK(initialized_);
  // We allocate the handle as a uint8_t array, then we call a placement new,
  // which calls the constructor. For symmetry, we call the destructor and then
  // delete on the uint8_t array.
  LIRSHandle* h = static_cast<LIRSHandle*>(handle);
  h->~LIRSHandle();
  uint8_t* data = reinterpret_cast<uint8_t*>(handle);
  delete [] data;
}

HandleBase* LIRSCacheShard::Lookup(const Slice& key, uint32_t hash,
    bool no_updates) {
  DCHECK(initialized_);
  LIRSHandle* e;
  LIRSThreadState tstate;
  {
    std::lock_guard<MutexType> l(mutex_);
    e = static_cast<LIRSHandle*>(table_.Lookup(key, hash));
    if (e != nullptr) {
      CHECK_NE(e->state(), UNINITIALIZED);
      // If the handle is a TOMBSTONE, Lookup() should pretend the entry doesn't exist.
      // TOMBSTONE has special treatment in Insert(); no other action is necessary here.
      if (e->state() == TOMBSTONE) return nullptr;
      e->get_reference();
      // If this is a no update lookup, nothing has changed. We can just return the
      // entry.
      if (no_updates) return e;
      switch (e->state()) {
      case UNINITIALIZED:
        // Needed to keep clang-tidy happy
        CHECK(false);
      case PROTECTED:
        // The PROTECTED entry is in the recency list, and the only action is to make
        // it the most recent element in the list. This can result in evictions if it is
        // currently the oldest entry.
        MoveToRecencyListBack(&tstate, e);
        break;
      case UNPROTECTED:
        if (e->recency_list_hook_.is_linked()) {
          // If an UNPROTECTED entry is in the recency list when it is referenced,
          // then we know that its new reuse distance is shorter than the last PROTECTED
          // entry in the list. So, this entry is upgraded from UNPROTECTED to
          // PROTECTED.
          UnprotectedToProtected(&tstate, e);
        } else {
          // If an UNPROTECTED entry is not on the recency list, then its new reuse
          // distance is longer than any of the PROTECTED entries. So, it remains
          // an UNPROTECTED entry, but it should be added back to the recency list and
          // readded as the most recent entry on the unprotected list.
          DCHECK_GT(num_unprotected_, 0);
          MoveToRecencyListBack(&tstate, e, false);
          if (num_unprotected_ != 1) {
            RemoveFromUnprotectedList(e);
            AddToUnprotectedList(e);
          }
        }
        break;
      default:
        CHECK(false) << "Unexpected state for Lookup: " << e->state();
      }
    }
  }

  // This happens when not holding the mutex for performance reasons.
  CleanupThreadState(&tstate);

  return e;
}

void LIRSCacheShard::UpdateCharge(HandleBase* handle, size_t charge) {
  DCHECK(initialized_);
  LIRSThreadState tstate;
  {
    // Hold the lock to avoid concurrent evictions
    std::lock_guard<MutexType> l(mutex_);
    LIRSHandle* e = static_cast<LIRSHandle*>(handle);
    // Entries that are UNINITIALIZED or TOMBSTONE do not count towards
    // the usage and will be destroyed without interacting with the usage.
    // Skip modifying the charge.
    //
    // In the case of UNINITIALIZED, we know that the caller has a handle
    // for the entry, so it needs to have been in the cache previously
    // (i.e. it was found via Lookup() or added via Insert()). So, in this
    // context, UNINITIALIZED can only mean that it has been evicted.
    if (e->state() == UNINITIALIZED || e->state() == TOMBSTONE) {
      return;
    }
    DCHECK(e->state() == PROTECTED || e->state() == UNPROTECTED);
    int64_t delta = charge - handle->charge();
    handle->set_charge(charge);
    UpdateMemTracker(delta);
    // Update usage in existing state, then evict as needed.
    switch (e->state()) {
    case PROTECTED:
      protected_usage_ += delta;
      EnforceProtectedCapacity(&tstate);
      break;
    case UNPROTECTED:
      unprotected_usage_ += delta;
      EnforceUnprotectedCapacity(&tstate);
      break;
    default:
      // This can't happen.
      CHECK(false) << "Unexpected state for UpdateCharge: " << e->state();
      break;
    }
  }
  CleanupThreadState(&tstate);
}

void LIRSCacheShard::Release(HandleBase* handle) {
  DCHECK(initialized_);
  LIRSHandle* e = static_cast<LIRSHandle*>(handle);
  LIRSThreadState tstate;

  auto state_transition = e->modify_atomic_state([] (AtomicState& cur_state) {
      DCHECK_GT(cur_state.ref_count, 0);
      --cur_state.ref_count;
      if (cur_state.ref_count == 0) {
        if (cur_state.state == TOMBSTONE || cur_state.state == UNINITIALIZED) {
          // An entry cannot be evicted until the reference count is zero, so this
          // must still be resident. In this circumstance, this thread must do the
          // eviction.
          DCHECK_EQ(cur_state.residency, RESIDENT);
          cur_state.residency = EVICTING;
        }
      }
    });
  // If we set the residency to EVICTING, then do the eviction. If this is UNINITIALIZED,
  // this will also free it.
  if (state_transition.after.residency == EVICTING) {
    tstate.handles_to_evict.push_back(e);
    CleanupThreadState(&tstate);
  }
}

HandleBase* LIRSCacheShard::Insert(HandleBase* e_in,
    Cache::EvictionCallback *eviction_callback) {
  DCHECK(initialized_);
  LIRSHandle* e = static_cast<LIRSHandle*>(e_in);
  CHECK_EQ(e->state(), UNINITIALIZED);
  // Set the remaining LIRSHandle members which were not already set in the constructor.
  e->eviction_callback_ = eviction_callback;
  // The caller has already stored the value in the handle (and any underlying storage).
  // Whether the Insert() is ultimately successful or not, this handle is currently
  // resident.
  e->set_resident();

  // Insert() can fail. A failed insert is equivalent to a successful insert followed
  // by an immediate eviction of that entry, so much of the setup code is identical.
  bool success = true;
  LIRSThreadState tstate;
  {
    std::lock_guard<MutexType> l(mutex_);

    // Cases:
    // 1. There is no existing entry.
    // 2. There is a tombstone entry.
    // 3. There is a non-tombstone entry (rare)
    // In any case, the existing entry will be replaced, so go ahead and remove it.
    LIRSHandle* existing_entry =
        static_cast<LIRSHandle*>(table_.Remove(e->key(), e->hash()));

    // The entry is always resident at the start of Insert(), so incorporate this
    // entry's charge into the memtracker. If Insert() fails, this is decremented as
    // part of eviction.
    UpdateMemTracker(e->charge());
    // All of these paths can result in evictions
    if (existing_entry != nullptr) {
      // Priorities are modified in Lookup, but priorities are not changed in Insert.
      // The exception is TOMBSTONE entries, which are not impacted by Lookup.
      // So:
      // 1. TOMBSTONE becomes PROTECTED.
      // 2. PROTECTED remains PROTECTED.
      // 3. UNPROTECTED remains UNPROTECTED.
      LIRSState existing_state = existing_entry->state();
      ToUninitialized(&tstate, existing_entry);
      if (existing_state == PROTECTED || existing_state == TOMBSTONE) {
        UninitializedToProtected(&tstate, e);
      } else {
        DCHECK_EQ(existing_state, UNPROTECTED);
        // UninitializedToUnprotected can fail (i.e. the entry inserted may not remain
        // in the cache).
        success = UninitializedToUnprotected(&tstate, e);
      }
    } else if (protected_usage_ + e->charge() <= protected_capacity_) {
      // There is available space in the protected area, so add it directly there.
      UninitializedToProtected(&tstate, e);
    } else {
      // UninitializedToUnprotected can fail (i.e. the entry inserted may not remain in
      // the cache).
      success = UninitializedToUnprotected(&tstate, e);
    }
    // If success=false, the entry is scheduled for eviction and won't be returned.
    if (success) {
      // Increment reference, since this will be returned.
      e->get_reference();
    }
  }

  // This happens when not holding the mutex for performance reasons.
  CleanupThreadState(&tstate);

  return (success ? e : nullptr);
}

void LIRSCacheShard::CleanupThreadState(LIRSThreadState* tstate) {
  // evict things that need to be evicted
  for (LIRSHandle* e : tstate->handles_to_evict) {
    DCHECK_EQ(e->residency(), EVICTING);
    DCHECK_EQ(e->num_references(), 0);
    if (e->eviction_callback_) {
      e->eviction_callback_->EvictedEntry(e->key(), e->value());
    }
    UpdateMemTracker(-static_cast<int64_t>(e->charge()));
    // The eviction is done, set the residency back to NOT_RESIDENT
    auto state_transition = e->modify_atomic_state([] (AtomicState& cur_state) {
        DCHECK_EQ(cur_state.residency, EVICTING);
        DCHECK_EQ(cur_state.ref_count, 0);
        cur_state.residency = NOT_RESIDENT;
      });
    // If the handle transitioned to UNINITIALIZED before we set it to NOT_RESIDENT,
    // then we are responsible for freeing it.
    if (state_transition.before.state == UNINITIALIZED) {
      Free(e);
    }
  }
  tstate->handles_to_evict.clear();
  for (LIRSHandle* e : tstate->handles_to_free) {
    Free(e);
  }
  tstate->handles_to_free.clear();
}

void LIRSCacheShard::Erase(const Slice& key, uint32_t hash) {
  DCHECK(initialized_);
  LIRSThreadState tstate;
  {
    std::lock_guard<MutexType> l(mutex_);
    LIRSHandle* e = static_cast<LIRSHandle*>(table_.Remove(key, hash));
    if (e != nullptr) {
      // Transition from any state to UNINITIALIZED. There may be external references, so
      // this might not actually free it immediately.
      ToUninitialized(&tstate, e);
    }
  }

  // This happens when not holding the mutex for performance reasons.
  CleanupThreadState(&tstate);
}

size_t LIRSCacheShard::Invalidate(const Cache::InvalidationControl& ctl) {
  DCHECK(initialized_);
  DCHECK(false) << "Invalidate() is not implemented for LIRS";
  return 0;
}

vector<HandleBase*> LIRSCacheShard::Dump() {
  DCHECK(initialized_);
  std::lock_guard<MutexType> l(mutex_);

  // For LIRS cache we only collect resident entries (i.e. PROTECTED/UNPROTECTED entries),
  // and ignore entries that are not resident (i.e. TOMBSTONE entries).
  vector<HandleBase*> handles;

  for (LIRSHandle& h : recency_list_) {
    // First walk through 'recency_list_', only collecting PROTECTED entries, ignoring
    // the UNPROTECTED/TOMBSTONE entries. UNPROTECTED entries will be collected later from
    // the unprotected_tombstone_list_.
    if (h.state() == PROTECTED) {
      h.get_reference();
      handles.push_back(&h);
    }
  }

  if (unprotected_list_front_ != nullptr) {
    DCHECK(unprotected_list_front_->unprotected_tombstone_list_hook_.is_linked());
    // From 'unprotected_list_front_' to the end of 'unprotected_tombstone_list_' are all
    // UNPROTECTED entries, collecting them all.
    auto iter = unprotected_tombstone_list_.iterator_to(*unprotected_list_front_);
    while (iter != unprotected_tombstone_list_.end()) {
      iter->get_reference();
      handles.push_back(&*iter);
      ++iter;
    }
  }

  return handles;
}

}  // end anonymous namespace

template<>
CacheShard* NewCacheShardInt<Cache::EvictionPolicy::LIRS>(kudu::MemTracker* mem_tracker,
    size_t capacity) {
  return new LIRSCacheShard(mem_tracker, capacity);
}

}  // namespace impala
