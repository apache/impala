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

#include "kudu/gutil/bits.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/alignment.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"
#include "util/cache/cache-internal.h"

// Useful in tests that require accurate cache capacity accounting.
DECLARE_bool(cache_force_single_shard);

DECLARE_double(cache_memtracker_approximation_ratio);

using std::atomic;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace impala {

Cache::~Cache() {
}

const Cache::ValidityFunc Cache::kInvalidateAllEntriesFunc = [](
    Slice /* key */, Slice /* value */) {
  return false;
};

const Cache::IterationFunc Cache::kIterateOverAllEntriesFunc = [](
    size_t /* valid_entries_num */, size_t /* invalid_entries_num */) {
  return true;
};

namespace {

// Recency list cache implementations (FIFO, LRU, etc.)

// Recency list handle. An entry is a variable length heap-allocated structure.
// Entries are kept in a circular doubly linked list ordered by some recency
// criterion (e.g., access time for LRU policy, insertion time for FIFO policy).
class RLHandle : public HandleBase {
public:
  RLHandle(uint8_t* kv_ptr, const Slice& key, int32_t hash, int val_len, int charge)
    : HandleBase(kv_ptr, key, hash, val_len, charge) {}

  RLHandle()
    : HandleBase(nullptr, Slice(), 0, 0, 0) {}

  Cache::EvictionCallback* eviction_callback;
  RLHandle* next;
  RLHandle* prev;
  std::atomic<int32_t> refs;
};

// A single shard of a cache that uses a recency list based eviction policy
template<Cache::EvictionPolicy policy>
class RLCacheShard : public CacheShard {
  static_assert(
      policy == Cache::EvictionPolicy::LRU || policy == Cache::EvictionPolicy::FIFO,
      "RLCacheShard only supports LRU or FIFO");

 public:
  explicit RLCacheShard(kudu::MemTracker* tracker);
  ~RLCacheShard();

  // Separate from constructor so caller can easily make an array of CacheShard
  void SetCapacity(size_t capacity) override {
    capacity_ = capacity;
    max_deferred_consumption_ = capacity * FLAGS_cache_memtracker_approximation_ratio;
  }

  HandleBase* Allocate(Slice key, uint32_t hash, int val_len, int charge) override;
  void Free(HandleBase* handle) override;
  HandleBase* Insert(HandleBase* handle,
      Cache::EvictionCallback* eviction_callback) override;
  HandleBase* Lookup(const Slice& key, uint32_t hash, bool caching) override;
  void Release(HandleBase* handle) override;
  void Erase(const Slice& key, uint32_t hash) override;
  size_t Invalidate(const Cache::InvalidationControl& ctl) override;

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

  // Initialized before use.
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
RLCacheShard<policy>::RLCacheShard(kudu::MemTracker* tracker)
  : usage_(0),
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
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
  mem_tracker_->Consume(deferred_consumption_);
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
    int charge) {
  int key_len = key.size();
  DCHECK_GE(key_len, 0);
  DCHECK_GE(val_len, 0);
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
  // We allocate the handle as a uint8_t array, then we call a placement new,
  // which calls the constructor. For symmetry, we call the destructor and then
  // delete on the uint8_t array.
  RLHandle* h = static_cast<RLHandle*>(handle);
  h->~RLHandle();
  uint8_t* data = reinterpret_cast<uint8_t*>(handle);
  delete [] data;
}

template<Cache::EvictionPolicy policy>
HandleBase* RLCacheShard<policy>::Lookup(const Slice& key,
                                         uint32_t hash,
                                         bool caching) {
  RLHandle* e;
  {
    std::lock_guard<decltype(mutex_)> l(mutex_);
    e = static_cast<RLHandle*>(table_.Lookup(key, hash));
    if (e != nullptr) {
      e->refs.fetch_add(1, std::memory_order_relaxed);
      RL_UpdateAfterLookup(e);
    }
  }

  return e;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::Release(HandleBase* handle) {
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
  RLHandle* handle = static_cast<RLHandle*>(handle_in);
  // Set the remaining RLHandle members which were not already allocated during
  // Allocate().
  handle->eviction_callback = eviction_callback;
  // Two refs for the handle: one from RLCacheShard, one for the returned handle.
  handle->refs.store(2, std::memory_order_relaxed);
  UpdateMemTracker(handle->charge());

  RLHandle* to_remove_head = nullptr;
  {
    std::lock_guard<decltype(mutex_)> l(mutex_);

    RL_Append(handle);

    RLHandle* old = static_cast<RLHandle*>(table_.Insert(handle));
    if (old != nullptr) {
      RL_Remove(old);
      if (Unref(old)) {
        old->next = to_remove_head;
        to_remove_head = old;
      }
    }

    while (usage_ > capacity_ && rl_.next != &rl_) {
      RLHandle* old = rl_.next;
      RL_Remove(old);
      table_.Remove(old->key(), old->hash());
      if (Unref(old)) {
        old->next = to_remove_head;
        to_remove_head = old;
      }
    }
  }

  // we free the entries here outside of mutex for
  // performance reasons
  while (to_remove_head != nullptr) {
    RLHandle* next = to_remove_head->next;
    FreeEntry(to_remove_head);
    to_remove_head = next;
  }

  return handle;
}

template<Cache::EvictionPolicy policy>
void RLCacheShard<policy>::Erase(const Slice& key, uint32_t hash) {
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
  size_t invalid_entry_count = 0;
  size_t valid_entry_count = 0;
  RLHandle* to_remove_head = nullptr;

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
        h_to_remove->next = to_remove_head;
        to_remove_head = h_to_remove;
      }
      ++invalid_entry_count;
    }
  }
  // Once removed from the lookup table and the recency list, the entries
  // with no references left must be deallocated because Cache::Release()
  // wont be called for them from elsewhere.
  while (to_remove_head != nullptr) {
    RLHandle* next = to_remove_head->next;
    FreeEntry(to_remove_head);
    to_remove_head = next;
  }
  return invalid_entry_count;
}

}  // end anonymous namespace

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int DetermineShardBits() {
  int bits = PREDICT_FALSE(FLAGS_cache_force_single_shard) ?
      0 : Bits::Log2Ceiling(base::NumCPUs());
  VLOG(1) << "Will use " << (1 << bits) << " shards for recency list cache.";
  return bits;
}

string ToString(Cache::EvictionPolicy p) {
  switch (p) {
    case Cache::EvictionPolicy::FIFO:
      return "fifo";
    case Cache::EvictionPolicy::LRU:
      return "lru";
    default:
      LOG(FATAL) << "unexpected cache eviction policy: " << static_cast<int>(p);
  }
  return "unknown";
}

template<>
Cache* NewCache<Cache::EvictionPolicy::FIFO,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id) {
  return new ShardedCache<Cache::EvictionPolicy::FIFO>(capacity, id);
}

template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id) {
  return new ShardedCache<Cache::EvictionPolicy::LRU>(capacity, id);
}

template<Cache::EvictionPolicy policy>
CacheShard* NewCacheShard(kudu::MemTracker* mem_tracker) {
  return new RLCacheShard<policy>(mem_tracker);
}

std::ostream& operator<<(std::ostream& os, Cache::MemoryType mem_type) {
  switch (mem_type) {
    case Cache::MemoryType::DRAM:
      os << "DRAM";
      break;
    default:
      os << "unknown (" << static_cast<int>(mem_type) << ")";
      break;
  }
  return os;
}

}  // namespace impala
