// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/cache/cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/status.h"
#include "gutil/bits.h"
#include "gutil/hash/city.h"
#include "gutil/macros.h"
#include "gutil/mathlimits.h"
#include "gutil/port.h"
#include "gutil/ref_counted.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/sysinfo.h"
#include "kudu/util/alignment.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"
#include "util/cache/cache-internal.h"

DECLARE_double(cache_memtracker_approximation_ratio);

using std::atomic;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

using strings::Substitute;

namespace impala {

namespace {

// Recency list cache implementations (FIFO, LRU, etc.)

// Recency list handle. An entry is a variable length heap-allocated structure.
// Entries are kept in a circular doubly linked list ordered by some recency
// criterion (e.g., access time for LRU policy, insertion time for FIFO policy).
class RLHandle : public HandleBase {
public:
  RLHandle(uint8_t* kv_ptr, const Slice& key, int32_t hash, int val_len, size_t charge)
    : HandleBase(kv_ptr, key, hash, val_len, charge) {
    refs.store(0);
    next = nullptr;
    prev = nullptr;
    eviction_callback = nullptr;
  }

  RLHandle()
    : HandleBase(nullptr, Slice(), 0, 0, 0) {}

  Cache::EvictionCallback* eviction_callback;
  RLHandle* next;
  RLHandle* prev;
  std::atomic<int32_t> refs;
};

struct RLThreadState {
  // Head for a linked-list of handles to evict. Eviction is done without holding
  // a mutex.
  RLHandle* to_remove_head = nullptr;
};

// A single shard of a cache that uses a recency list based eviction policy
template<Cache::EvictionPolicy policy>
class RLCacheShard : public CacheShard {
  static_assert(
      policy == Cache::EvictionPolicy::LRU || policy == Cache::EvictionPolicy::FIFO,
      "RLCacheShard only supports LRU or FIFO");

 public:
  explicit RLCacheShard(kudu::MemTracker* tracker, size_t capacity);
  ~RLCacheShard();

  Status Init() override;
  HandleBase* Allocate(Slice key, uint32_t hash, int val_len, size_t charge) override;
  void Free(HandleBase* handle) override;
  HandleBase* Insert(HandleBase* handle,
      Cache::EvictionCallback* eviction_callback) override;
  HandleBase* Lookup(const Slice& key, uint32_t hash, bool caching) override;
  void UpdateCharge(HandleBase* handle, size_t charge) override;
  void Release(HandleBase* handle) override;
  void Erase(const Slice& key, uint32_t hash) override;
  size_t Invalidate(const Cache::InvalidationControl& ctl) override;
  vector<HandleBase*> Dump() override;

 private:
  void RL_Remove(RLHandle* e);
  void RL_Append(RLHandle* e);
  // Update the recency list after a lookup operation.
  void RL_UpdateAfterLookup(RLHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(RLHandle* e);
  // Call the user's eviction callback, if it exists, and free the entry.
  void FreeEntry(RLHandle* e);

  // This enforces the capacity constraint for the cache. This should be
  // called while holding the cache's lock. This accumulates evicted
  // entries in the provided RLThreadState, which should be freed
  // via CleanupThreadState().
  void EnforceCapacity(RLThreadState* tstate);

  // Functions move evictions outside the critical section for mutex_ by
  // placing the affected entries on the LIRSThreadState. This function handles the
  // accumulated evictions. It should be called without holding any locks.
  void CleanupThreadState(RLThreadState* tstate);

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

  bool initialized_ = false;

  size_t capacity_;

  // mutex_ protects the following state.
  kudu::simple_spinlock mutex_;
  size_t usage_;

  // Dummy head of recency list.
  // rl.prev is newest entry, rl.next is oldest entry.
  RLHandle rl_;

  HandleTable table_;

  kudu::MemTracker* mem_tracker_;
  atomic<int64_t> deferred_consumption_ { 0 };

  // Initialized based on capacity_ to ensure an upper bound on the error on the
  // MemTracker consumption.
  int64_t max_deferred_consumption_;
};

template<Cache::EvictionPolicy policy>
RLCacheShard<policy>::RLCacheShard(kudu::MemTracker* tracker, size_t capacity)
  : capacity_(capacity),
    usage_(0),
    mem_tracker_(tracker) {
  // Make empty circular linked list.
  rl_.next = &rl_;
  rl_.prev = &rl_;
}

template<Cache::EvictionPolicy policy>
RLCacheShard<policy>::~RLCacheShard() {
  for (RLHandle* e = rl_.next; e != &rl_; ) {
    RLHandle* next = e->next;
    DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 1)
        << "caller has an unreleased handle";
    table_.Remove(e->key(), e->hash());
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
  mem_tracker_->Consume(deferred_consumption_);
}

template<Cache::EvictionPolicy policy>
Status RLCacheShard<policy>::Init() {
  if (!MathLimits<double>::IsFinite(FLAGS_cache_memtracker_approximation_ratio) ||
      FLAGS_cache_memtracker_approximation_ratio < 0.0 ||
      FLAGS_cache_memtracker_approximation_ratio > 1.0) {
    return Status(Substitute("Misconfigured --cache_memtracker_approximation_ratio: $0. "
        "Must be between 0 and 1.", FLAGS_cache_memtracker_approximation_ratio));
  }
  max_deferred_consumption_ = capacity_ * FLAGS_cache_memtracker_approximation_ratio;
  initialized_ = true;
  return Status::OK();
}

template<Cache::EvictionPolicy policy>
bool RLCacheShard<policy>::Unref(RLHandle* e) {
  DCHECK_GT(e->refs.load(std::memory_order_relaxed), 0);
  return e->refs.fetch_sub(1) == 1;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::FreeEntry(RLHandle* e) {
  DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 0);
  if (e->eviction_callback) {
    e->eviction_callback->EvictedEntry(e->key(), e->value());
  }
  UpdateMemTracker(-static_cast<int64_t>(e->charge()));
  Free(e);
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::UpdateMemTracker(int64_t delta) {
  int64_t old_deferred = deferred_consumption_.fetch_add(delta);
  int64_t new_deferred = old_deferred + delta;

  if (new_deferred > max_deferred_consumption_ ||
      new_deferred < -max_deferred_consumption_) {
    int64_t to_propagate = deferred_consumption_.exchange(0, std::memory_order_relaxed);
    mem_tracker_->Consume(to_propagate);
  }
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::RL_Remove(RLHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  // Null out the next/prev to make it easy to tell that a handle is
  // not part of the cache.
  e->next = nullptr;
  e->prev = nullptr;
  DCHECK_GE(usage_, e->charge());
  usage_ -= e->charge();
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::RL_Append(RLHandle* e) {
  // Make "e" newest entry by inserting just before rl_.
  e->next = &rl_;
  e->prev = rl_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge();
}

template<>
void RLCacheShard<Cache::EvictionPolicy::FIFO>::RL_UpdateAfterLookup(RLHandle* /* e */) {
}

template<>
void RLCacheShard<Cache::EvictionPolicy::LRU>::RL_UpdateAfterLookup(RLHandle* e) {
  RL_Remove(e);
  RL_Append(e);
}

template<Cache::EvictionPolicy policy>
HandleBase* RLCacheShard<policy>::Allocate(Slice key, uint32_t hash, int val_len,
    size_t charge) {
  DCHECK(initialized_);
  int key_len = key.size();
  DCHECK_GE(key_len, 0);
  DCHECK_GE(val_len, 0);
  DCHECK_GT(charge, 0);
  if (charge == 0) return nullptr;
  int key_len_padded = KUDU_ALIGN_UP(key_len, sizeof(void*));
  uint8_t* buf = new uint8_t[sizeof(RLHandle)
                             + key_len_padded + val_len]; // the kv_data VLA data
  // TODO(KUDU-1091): account for the footprint of structures used by Cache's
  //                  internal housekeeping (RL handles, etc.) in case of
  //                  non-automatic charge.
  int calc_charge =
    (charge == Cache::kAutomaticCharge) ? kudu::kudu_malloc_usable_size(buf) : charge;
  uint8_t* kv_ptr = buf + sizeof(RLHandle);
  RLHandle* handle = new (buf) RLHandle(kv_ptr, key, hash, val_len,
      calc_charge);
  return handle;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::Free(HandleBase* handle) {
  DCHECK(initialized_);
  // We allocate the handle as a uint8_t array, then we call a placement new,
  // which calls the constructor. For symmetry, we call the destructor and then
  // delete on the uint8_t array.
  RLHandle* h = static_cast<RLHandle*>(handle);
  h->~RLHandle();
  uint8_t* data = reinterpret_cast<uint8_t*>(handle);
  delete [] data;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::EnforceCapacity(RLThreadState* tstate) {
  while (usage_ > capacity_ && rl_.next != &rl_) {
    RLHandle* old = rl_.next;
    RL_Remove(old);
    table_.Remove(old->key(), old->hash());
    if (Unref(old)) {
      old->next = tstate->to_remove_head;
      tstate->to_remove_head = old;
    }
  }
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::CleanupThreadState(RLThreadState* tstate) {
  while (tstate->to_remove_head != nullptr) {
    RLHandle* next = tstate->to_remove_head->next;
    FreeEntry(tstate->to_remove_head);
    tstate->to_remove_head = next;
  }
}

template<Cache::EvictionPolicy policy>
HandleBase* RLCacheShard<policy>::Lookup(const Slice& key,
                                         uint32_t hash,
                                         bool no_updates) {
  DCHECK(initialized_);
  RLHandle* e;
  {
    std::lock_guard<decltype(mutex_)> l(mutex_);
    e = static_cast<RLHandle*>(table_.Lookup(key, hash));
    if (e != nullptr) {
      e->refs.fetch_add(1, std::memory_order_relaxed);
      // If this is a no update lookup, skip the modifications.
      if (!no_updates) RL_UpdateAfterLookup(e);
    }
  }

  return e;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::UpdateCharge(HandleBase* handle, size_t charge) {
  DCHECK(initialized_);

  // Check for eviction based on new usage
  RLThreadState tstate;
  {
    // Hold the lock to avoid concurrent evictions
    std::lock_guard<decltype(mutex_)> l(mutex_);
    RLHandle* e = static_cast<RLHandle*>(handle);
    // If the handle is not part of the cache (i.e. it was evicted),
    // then we shouldn't update the charge. The caller has a handle to
    // the entry, so it was previously in the cache.
    if (e->next == nullptr) return;
    int64_t delta = charge - handle->charge();
    handle->set_charge(charge);
    UpdateMemTracker(delta);
    usage_ += delta;

    // Evict entries as needed to enforce the capacity constraint
    EnforceCapacity(&tstate);
  }

  // we free the entries here outside of mutex for performance reasons
  CleanupThreadState(&tstate);
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::Release(HandleBase* handle) {
  DCHECK(initialized_);
  RLHandle* e = static_cast<RLHandle*>(handle);
  bool last_reference = Unref(e);
  if (last_reference) {
    FreeEntry(e);
  }
}

template<Cache::EvictionPolicy policy>
HandleBase* RLCacheShard<policy>::Insert(
    HandleBase* handle_in,
    Cache::EvictionCallback* eviction_callback) {
  DCHECK(initialized_);
  RLHandle* handle = static_cast<RLHandle*>(handle_in);
  // Set the remaining RLHandle members which were not already allocated during
  // Allocate().
  handle->eviction_callback = eviction_callback;
  // Two refs for the handle: one from RLCacheShard, one for the returned handle.
  handle->refs.store(2, std::memory_order_relaxed);
  UpdateMemTracker(handle->charge());

  RLThreadState tstate;
  {
    std::lock_guard<decltype(mutex_)> l(mutex_);

    RL_Append(handle);

    RLHandle* old = static_cast<RLHandle*>(table_.Insert(handle));
    if (old != nullptr) {
      RL_Remove(old);
      if (Unref(old)) {
        old->next = tstate.to_remove_head;
        tstate.to_remove_head = old;
      }
    }

    // Evict entries as needed to enforce the capacity constraint
    EnforceCapacity(&tstate);
  }

  // we free the entries here outside of mutex for
  // performance reasons
  CleanupThreadState(&tstate);

  return handle;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::Erase(const Slice& key, uint32_t hash) {
  DCHECK(initialized_);
  RLHandle* e;
  bool last_reference = false;
  {
    std::lock_guard<decltype(mutex_)> l(mutex_);
    e = static_cast<RLHandle*>(table_.Remove(key, hash));
    if (e != nullptr) {
      RL_Remove(e);
      last_reference = Unref(e);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != NULL
  if (last_reference) {
    FreeEntry(e);
  }
}

template<Cache::EvictionPolicy policy>
size_t RLCacheShard<policy>::Invalidate(const Cache::InvalidationControl& ctl) {
  DCHECK(initialized_);
  size_t invalid_entry_count = 0;
  size_t valid_entry_count = 0;
  RLThreadState tstate;

  {
    std::lock_guard<decltype(mutex_)> l(mutex_);

    // rl_.next is the oldest (a.k.a. least relevant) entry in the recency list.
    RLHandle* h = rl_.next;
    while (h != nullptr && h != &rl_ &&
           ctl.iteration_func(valid_entry_count, invalid_entry_count)) {
      if (ctl.validity_func(h->key(), h->value())) {
        // Continue iterating over the list.
        h = h->next;
        ++valid_entry_count;
        continue;
      }
      // Copy the handle slated for removal.
      RLHandle* h_to_remove = h;
      // Prepare for next iteration of the cycle.
      h = h->next;

      RL_Remove(h_to_remove);
      table_.Remove(h_to_remove->key(), h_to_remove->hash());
      if (Unref(h_to_remove)) {
        h_to_remove->next = tstate.to_remove_head;
        tstate.to_remove_head = h_to_remove;
      }
      ++invalid_entry_count;
    }
  }
  // Once removed from the lookup table and the recency list, the entries
  // with no references left must be deallocated because Cache::Release()
  // wont be called for them from elsewhere.
  CleanupThreadState(&tstate);
  return invalid_entry_count;
}

template<Cache::EvictionPolicy policy>
vector<HandleBase*> RLCacheShard<policy>::Dump() {
  std::lock_guard<decltype(mutex_)> l(mutex_);
  vector<HandleBase*> handles;
  RLHandle* h = rl_.next;
  while (h != &rl_) {
    h->refs.fetch_add(1, std::memory_order_relaxed);
    handles.push_back(h);
    h = h->next;
  }
  return handles;
}

}  // end anonymous namespace

template<>
CacheShard* NewCacheShardInt<Cache::EvictionPolicy::FIFO>(kudu::MemTracker* mem_tracker,
    size_t capacity) {
  return new RLCacheShard<Cache::EvictionPolicy::FIFO>(mem_tracker, capacity);
}

template<>
CacheShard* NewCacheShardInt<Cache::EvictionPolicy::LRU>(kudu::MemTracker* mem_tracker,
    size_t capacity) {
  return new RLCacheShard<Cache::EvictionPolicy::LRU>(mem_tracker, capacity);
}

}  // namespace impala
