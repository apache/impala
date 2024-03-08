// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/cache/cache.h"

#include <vector>

#include "common/status.h"
#include "kudu/gutil/bits.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/alignment.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

using kudu::Slice;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;

namespace impala {

// Base class for handles for all algorithms. Sharing the base class allows for a shared
// HashTable implementation and a shared sharding implementation. HandleBase is designed
// to allow the entire handle to be stored in a single allocation, even though the key
// and value can be variable-sized. It does this by taking in a pointer to the key/value
// pair in the constructor. For a contiguous layout, the key/value pair would be stored
// immediately following the HandleBase (or subclass) and the caller of the constructor
// would pass in a pointer to this contiguous data.
class HandleBase {
 public:
  // Create a HandleBase. See Cache::Allocate() for information on the 'key', 'val_len',
  // and 'charge' parameters. The remaining parameters are:
  // - kv_ptr - points to memory for the variable sized key/value pair
  //   It must be sized to fit the key and value with appropriate padding. See the
  //   description of kv_data_ptr_ for the memory layout and sizing.
  // - hash - hash of the key
  HandleBase(uint8_t* kv_ptr, const Slice& key, int32_t hash, int val_len,
      size_t charge) {
    DCHECK_GE(charge, 0);
    next_handle_ = nullptr;
    key_length_ = key.size();
    val_length_ = val_len;
    hash_ = hash;
    charge_ = charge;
    kv_data_ptr_ = kv_ptr;
    if (kv_data_ptr_ != nullptr && key_length_ > 0) {
      memcpy(kv_data_ptr_, key.data(), key_length_);
    }
  }

  ~HandleBase() {
    DCHECK(next_handle_ == nullptr);
  }

  Slice key() const {
    return Slice(kv_data_ptr_, key_length_);
  }

  uint32_t hash() const { return hash_; }

  uint8_t* mutable_val_ptr() {
    int val_offset = KUDU_ALIGN_UP(key_length_, sizeof(void*));
    return &kv_data_ptr_[val_offset];
  }

  const uint8_t* val_ptr() const {
    return const_cast<HandleBase*>(this)->mutable_val_ptr();
  }

  Slice value() const {
    return Slice(val_ptr(), val_length_);
  }

  size_t charge() const { return charge_; }

  void set_charge(size_t charge) {
    charge_ = charge;
  }

 private:
  friend class HandleTable;
  HandleBase* next_handle_;
  uint32_t hash_;
  uint32_t key_length_;
  uint32_t val_length_;
  size_t charge_;

  // kv_data_ptr_ is a pointer to the beginning of the key/value pair. The key/value
  // pair is stored as a byte array with the format:
  //   [key bytes ...] [padding up to 8-byte boundary] [value bytes ...]
  uint8_t* kv_data_ptr_;
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  HandleBase* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  HandleBase* Insert(HandleBase* h) {
    HandleBase** ptr = FindPointer(h->key(), h->hash());
    HandleBase* old = *ptr;
    h->next_handle_ = (old == nullptr ? nullptr : old->next_handle_);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    } else {
      old->next_handle_ = nullptr;
    }
    return old;
  }

  HandleBase* Remove(const Slice& key, uint32_t hash) {
    HandleBase** ptr = FindPointer(key, hash);
    HandleBase* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_handle_;
      result->next_handle_ = nullptr;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  HandleBase** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  HandleBase** FindPointer(const Slice& key, uint32_t hash) {
    HandleBase** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr &&
           ((*ptr)->hash() != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_handle_;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 16;
    while (new_length < elems_ * 1.5) {
      new_length *= 2;
    }
    auto new_list = new HandleBase*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; ++i) {
      HandleBase* h = list_[i];
      while (h != nullptr) {
        HandleBase* next = h->next_handle_;
        uint32_t hash = h->hash();
        HandleBase** ptr = &new_list[hash & (new_length - 1)];
        h->next_handle_ = *ptr;
        *ptr = h;
        h = next;
        ++count;
      }
    }
    DCHECK_EQ(elems_, count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// This defines a single shared interface for the cache algorithms, which allows them to
// share the sharding implementation. There are several differences between this interface
// and the Cache interface:
// 1. The functions use HandleBase arguments rather than the types defined externally
//    for the Cache.
// 2. This does not use separate Handle vs PendingHandle types. Allocate and Free just use
//    HandleBase like all other functions. At the moment, no implementation uses this
//    functionality. This is internal code, so the distinction can be reintroduced later
//    if it is useful.
// 3. Since the sharding would already need the hash, it passes the computed hash through
//    for all functions where it makes sense.
class CacheShard {
 public:
  virtual ~CacheShard() {}

  // Initialization function. Must be called before any other function.
  virtual Status Init() = 0;

  virtual HandleBase* Allocate(Slice key, uint32_t hash, int val_len, size_t charge) = 0;
  virtual void Free(HandleBase* handle) = 0;
  virtual HandleBase* Insert(HandleBase* handle,
      Cache::EvictionCallback* eviction_callback) = 0;
  virtual HandleBase* Lookup(const Slice& key, uint32_t hash, bool no_updates) = 0;
  virtual void UpdateCharge(HandleBase* handle, size_t new_charge) = 0;
  virtual void Release(HandleBase* handle) = 0;
  virtual void Erase(const Slice& key, uint32_t hash) = 0;
  virtual size_t Invalidate(const Cache::InvalidationControl& ctl) = 0;
  virtual vector<HandleBase*> Dump() = 0;
};

// Function to build a cache shard using the given eviction algorithm.
// This untemplatized function allows ShardedCache to avoid templating and
// keeps the templates internal to the cache.
CacheShard* NewCacheShard(Cache::EvictionPolicy eviction_policy,
    kudu::MemTracker* mem_tracker, size_t capacity);

// Internal templatized function to build a cache shard for the given eviction
// algorithm. The template argument allows the different eviction policy implementations
// to be defined in different files (e.g. LRU in rl-cache.cc and LIRS in lirs-cache.cc).
template <Cache::EvictionPolicy eviction_policy>
CacheShard* NewCacheShardInt(kudu::MemTracker* mem_tracker, size_t capacity);

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int DetermineShardBits();

// Helper function to provide a string representation of an eviction policy.
string ToString(Cache::EvictionPolicy p);

// This is a minimal sharding cache implementation. It passes almost all functions
// through to the underlying CacheShard unless the function can be answered by the
// HandleBase itself. The number of shards is currently a power of 2 so that it can
// do a right shift to get the shard index.
class ShardedCache : public Cache {
 public:
  explicit ShardedCache(Cache::EvictionPolicy policy, size_t capacity, const string& id)
      : shard_bits_(DetermineShardBits()) {
    // A cache is often a singleton, so:
    // 1. We reuse its MemTracker if one already exists, and
    // 2. It is directly parented to the root MemTracker.
    // TODO: Change this to an Impala MemTracker, and only track the memory usage for the
    // metadata. This currently does not hook up to Impala's MemTracker hierarchy
    mem_tracker_ = kudu::MemTracker::FindOrCreateGlobalTracker(
        -1, strings::Substitute("$0-sharded_$1_cache", id, ToString(policy)));

    int num_shards = 1 << shard_bits_;
    const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
    for (int s = 0; s < num_shards; ++s) {
      shards_.push_back(NewCacheShard(policy, mem_tracker_.get(), per_shard));
    }

    if (per_shard < shard_charge_limit_) {
      shard_charge_limit_ = static_cast<int>(per_shard);
    }
  }

  virtual ~ShardedCache() {
    STLDeleteElements(&shards_);
  }

  Status Init() override {
    for (CacheShard* shard : shards_) {
      RETURN_IF_ERROR(shard->Init());
    }
    return Status::OK();
  }

  UniqueHandle Lookup(const Slice& key, LookupBehavior behavior) override {
    const uint32_t hash = HashSlice(key);
    HandleBase* h = shards_[Shard(hash)]->Lookup(key, hash, behavior == NO_UPDATE);
    return UniqueHandle(reinterpret_cast<Cache::Handle*>(h), Cache::HandleDeleter(this));
  }

  size_t Charge(const UniqueHandle& handle) override {
    return reinterpret_cast<HandleBase*>(handle.get())->charge();
  }

  void UpdateCharge(const UniqueHandle& handle, size_t charge) override {
    HandleBase* h = reinterpret_cast<HandleBase*>(handle.get());
    shards_[Shard(h->hash())]->UpdateCharge(h, charge);
    // NOTE: the handle could point to an entry that has been evicted, so there is no
    // guarantee that the charge will actually change. That's why we don't assert that
    // h->charge() == charge here.
  }

  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shards_[Shard(hash)]->Erase(key, hash);
  }

  Slice Key(const UniqueHandle& handle) const override {
    return reinterpret_cast<HandleBase*>(handle.get())->key();
  }

  Slice Value(const UniqueHandle& handle) const override {
    return reinterpret_cast<HandleBase*>(handle.get())->value();
  }

  UniqueHandle Insert(UniquePendingHandle handle,
      Cache::EvictionCallback* eviction_callback) override {
    HandleBase* h_in = reinterpret_cast<HandleBase*>(DCHECK_NOTNULL(handle.release()));
    HandleBase* h_out = shards_[Shard(h_in->hash())]->Insert(h_in, eviction_callback);
    return UniqueHandle(reinterpret_cast<Cache::Handle*>(h_out),
        Cache::HandleDeleter(this));
  }

  size_t MaxCharge() const override {
    return shard_charge_limit_;
  }

  UniquePendingHandle Allocate(Slice key, int val_len, size_t charge) override {
    const uint32_t hash = HashSlice(key);
    HandleBase* handle = shards_[Shard(hash)]->Allocate(key, hash, val_len, charge);
    UniquePendingHandle h(reinterpret_cast<PendingHandle*>(handle),
                          PendingHandleDeleter(this));

    return h;
  }

  uint8_t* MutableValue(UniquePendingHandle* handle) const override {
    return reinterpret_cast<HandleBase*>(handle->get())->mutable_val_ptr();
  }

  size_t Invalidate(const InvalidationControl& ctl) override {
    size_t invalidated_count = 0;
    for (auto& shard: shards_) {
      invalidated_count += shard->Invalidate(ctl);
    }
    return invalidated_count;
  }

  vector<UniqueHandle> Dump() override {
    vector<UniqueHandle> handles;
    for (auto& shard : shards_) {
      for (HandleBase* handle : shard->Dump()) {
        handles.emplace_back(
            reinterpret_cast<Cache::Handle*>(handle), Cache::HandleDeleter(this));
      }
    }
    return handles;
  }

 protected:
  void Release(Handle* handle) override {
    HandleBase* h = reinterpret_cast<HandleBase*>(handle);
    shards_[Shard(h->hash())]->Release(h);
  }

  void Free(PendingHandle* pending_handle) override {
    HandleBase* h = reinterpret_cast<HandleBase*>(pending_handle);
    shards_[Shard(h->hash())]->Free(h);
  }

 private:
  shared_ptr<kudu::MemTracker> mem_tracker_;
  vector<CacheShard*> shards_;

  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  // Max charge that can be stored in a shard.
  size_t shard_charge_limit_ = numeric_limits<size_t>::max();

  static inline uint32_t HashSlice(const Slice& s) {
    return util_hash::CityHash64(reinterpret_cast<const char *>(s.data()), s.size());
  }

  uint32_t Shard(uint32_t hash) {
    // Widen to uint64 before shifting, or else on a single CPU,
    // we would try to shift a uint32_t by 32 bits, which is undefined.
    return static_cast<uint64_t>(hash) >> (32 - shard_bits_);
  }
};

}
