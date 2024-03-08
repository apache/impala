// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// This is taken from LevelDB and evolved to fit the kudu codebase.
//
// TODO(unknown): this is pretty lock-heavy. Would be good to sub out something
// a little more concurrent.

#pragma once

#include <boost/algorithm/string.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"
#include "common/status.h"

using kudu::Slice;

namespace impala {

class Cache {
 public:
  // Supported eviction policies for the cache. Eviction policy determines what
  // items to evict if the cache is at capacity when trying to accommodate
  // an extra item.
  enum class EvictionPolicy {
    // The earliest added items are evicted (a.k.a. queue).
    FIFO,

    // The least-recently-used items are evicted.
    LRU,

    // LIRS (Low Inter-reference Recency Set)
    LIRS,
  };

  static EvictionPolicy ParseEvictionPolicy(const std::string& policy_string) {
    string upper_policy = boost::to_upper_copy(policy_string);
    if (upper_policy == "LRU") {
      return Cache::EvictionPolicy::LRU;
    } else if (upper_policy == "LIRS") {
      return Cache::EvictionPolicy::LIRS;
    } else if (upper_policy == "FIFO") {
      return Cache::EvictionPolicy::FIFO;
    }
    LOG(FATAL) << "Unsupported eviction policy: " << policy_string;
    return Cache::EvictionPolicy::LRU;
  }

  // Callback interface which is called when an entry is evicted from the
  // cache.
  class EvictionCallback {
   public:
    virtual void EvictedEntry(Slice key, Slice value) = 0;
    virtual ~EvictionCallback() = default;
  };

  Cache() = default;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Initialization function for the cache. This must be called before using
  // any other methods. It returns error status if there is any issue with
  // the cache or its parameters.
  virtual Status Init() = 0;

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Custom deleter: intended for use with std::unique_ptr<Handle>.
  class HandleDeleter {
   public:
    explicit HandleDeleter(Cache* c)
        : c_(c) {
    }

    // It is useful to have a constructor without arguments, as
    // this makes it easier to embed in containers.
    explicit HandleDeleter()
      : c_(nullptr) {}

    void operator()(Cache::Handle* h) const {
      if (h != nullptr) {
        DCHECK(c_ != nullptr);
        c_->Release(h);
      }
    }

    Cache* cache() const {
      return c_;
    }

   private:
    Cache* c_;
  };

  // UniqueHandle -- a wrapper around opaque Handle structure to facilitate
  // automatic reference counting of cache's handles.
  typedef std::unique_ptr<Handle, HandleDeleter> UniqueHandle;

  // Opaque handle to an entry which is being prepared to be added to the cache.
  struct PendingHandle { };

  // Custom deleter: intended for use with std::unique_ptr<PendingHandle>.
  class PendingHandleDeleter {
   public:
    explicit PendingHandleDeleter(Cache* c)
        : c_(c) {
    }

    // It is useful to have a constructor without arguments, as
    // this makes it easier to embed in containers.
    explicit PendingHandleDeleter()
      : c_(nullptr) {}

    void operator()(Cache::PendingHandle* h) const {
      if (h != nullptr) {
        DCHECK(c_ != nullptr);
        c_->Free(h);
      }
    }

    Cache* cache() const {
      return c_;
    }

   private:
    Cache* c_;
  };

  // UniquePendingHandle -- a wrapper around opaque PendingHandle structure
  // to facilitate automatic reference counting newly allocated cache's handles.
  typedef std::unique_ptr<PendingHandle, PendingHandleDeleter> UniquePendingHandle;

  // There are times when callers may want to perform a Lookup() without having it
  // impact the priorities of the cache elements. For example, if the caller is
  // doing a Lookup() as an internal part of its implementation that does not
  // correspond to actual user activity (or duplicates user activity).
  //
  // When LookupBehavior is NORMAL (the default), the cache policy will update the
  // priorities of entries. When LookupBehavior is NO_UPDATE, the cache policy will
  // not update the priority of any entry.
  enum LookupBehavior {
    NORMAL,
    NO_UPDATE
  };

  // If the cache has no mapping for "key", returns NULL.
  //
  // Else return a handle that corresponds to the mapping.
  //
  // Handles are not intended to be held for significant periods of time. Also,
  // threads should avoid getting multiple handles for the same key simultaneously.
  //
  // Sample usage:
  //
  //   unique_ptr<Cache> cache(NewCache(...));
  //   ...
  //   {
  //     Cache::UniqueHandle h(cache->Lookup(...)));
  //     ...
  //   } // 'h' is automatically released here
  //
  // Or:
  //
  //   unique_ptr<Cache> cache(NewCache(...));
  //   ...
  //   {
  //     auto h(cache->Lookup(...)));
  //     ...
  //   } // 'h' is automatically released here
  //
  virtual UniqueHandle Lookup(const Slice& key, LookupBehavior behavior = NORMAL) = 0;

  // Return the charge encapsulate in a raw handle returned by a successful
  // Lookup().
  virtual size_t Charge(const UniqueHandle& handle) = 0;

  // Update the charge of a handle after insertion.
  virtual void UpdateCharge(const UniqueHandle& handle, size_t charge) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return the key encapsulated in a raw handle returned by a successful
  // Lookup().
  virtual Slice Key(const UniqueHandle& handle) const = 0;

  // Return the value encapsulated in a raw handle returned by a successful
  // Lookup().
  virtual Slice Value(const UniqueHandle& handle) const = 0;


  // ------------------------------------------------------------
  // Insertion path
  // ------------------------------------------------------------
  //
  // Because some cache implementations (eg NVM) manage their own memory, and because we'd
  // like to read blocks directly into cache-managed memory rather than causing an extra
  // memcpy, the insertion of a new element into the cache requires two phases. First, a
  // PendingHandle is allocated with space for the value, and then it is later inserted.
  //
  // For example:
  //
  //   auto ph(cache_->Allocate("my entry", value_size, charge));
  //   if (!ReadDataFromDisk(cache_->MutableValue(&ph)).ok()) {
  //     ... error handling ...
  //     return;
  //   }
  //   UniqueHandle h(cache_->Insert(std::move(ph), my_eviction_callback));
  //   ...
  //   // 'h' is automatically released.

  // Indicates that the charge of an item in the cache should be calculated
  // based on its memory consumption.
  static constexpr size_t kAutomaticCharge = std::numeric_limits<size_t>::max();

  // Returns the maximum charge that will be accepted by this cache.
  // Can be used to avoid generating data that we won't be able to insert.
  virtual size_t MaxCharge() const = 0;

  // Allocate space for a new entry to be inserted into the cache.
  //
  // The provided 'key' is copied into the resulting handle object.
  // The allocated handle has enough space such that the value can
  // be written into cache_->MutableValue(&handle).
  //
  // If 'charge' is not 'kAutomaticCharge', then the cache capacity will be charged
  // the explicit amount. This is useful when caching items that are small but need to
  // maintain a bounded count (eg file descriptors) rather than caring about their actual
  // memory usage. It is also useful when caching items for whom calculating
  // memory usage is a complex affair (i.e. items containing pointers to
  // additional heap allocations).
  //
  // Note that this does not mutate the cache itself: lookups will
  // not be able to find the provided key until it is inserted.
  //
  // It is possible that this will return a nullptr wrapped in a std::unique_ptr
  // if the cache is above its capacity and eviction fails to free up enough
  // space for the requested allocation.
  //
  // The returned handle owns the allocated memory.
  virtual UniquePendingHandle Allocate(Slice key, int val_len, size_t charge) = 0;

  // Default 'charge' should be kAutomaticCharge
  // (default arguments on virtual functions are prohibited).
  UniquePendingHandle Allocate(Slice key, int val_len) {
    return Allocate(key, val_len, kAutomaticCharge);
  }

  // Get the mutable space created by Allocate.
  virtual uint8_t* MutableValue(UniquePendingHandle* handle) const = 0;

  // Commit a prepared entry into the cache.
  //
  // The 'pending' entry passed here should have been allocated using
  // Cache::Allocate() above.
  //
  // This method is not guaranteed to succeed. If it succeeds, it returns a handle
  // that corresponds to the mapping. If it fails, it returns a null handle.
  //
  // Handles are not intended to be held for significant periods of time. Also,
  // threads should avoid getting multiple handles for the same key simultaneously.
  //
  // If 'eviction_callback' is non-NULL, then it will be called when the
  // entry is later evicted or when the cache shuts down. If the Insert()
  // fails, the entry is immediately destroyed and the eviction callback is called
  // before the return of Insert().
  virtual UniqueHandle Insert(UniquePendingHandle pending,
                              EvictionCallback* eviction_callback) = 0;

  // Forward declaration to simplify the layout of types/typedefs needed for the
  // Invalidate() method while trying to adhere to the code style guide.
  struct InvalidationControl;

  // Invalidate cache's entries, effectively evicting non-valid ones from the
  // cache. The invalidation process iterates over the cache's recency list(s),
  // from best candidate for eviction to the worst.
  //
  // The provided control structure 'ctl' is responsible for the following:
  //   * determine whether an entry is valid or not
  //   * determine how to iterate over the entries in the cache's recency list
  //
  // NOTE: The invalidation process might hold a lock while iterating over
  //       the cache's entries. Using proper IterationFunc might help to reduce
  //       contention with the concurrent request for the cache's contents.
  //       See the in-line documentation for IterationFunc for more details.
  virtual size_t Invalidate(const InvalidationControl& ctl) = 0;

  // Walk through all valid entries in the cache and push their handles into the return
  // vector.
  virtual std::vector<UniqueHandle> Dump() = 0;

  // Functor to define a criterion on a cache entry's validity. Upon call
  // of Cache::Invalidate() method, if the functor returns 'false' for the
  // specified key and value, the cache evicts the entry, otherwise the entry
  // stays in the cache.
  typedef std::function<bool(Slice /* key */,
                             Slice /* value */)>
      ValidityFunc;

  // Functor to define whether to continue or stop iterating over the cache's
  // entries based on the number of encountered invalid and valid entries
  // during the Cache::Invalidate() call. If a cache contains multiple
  // sub-caches (e.g., shards), those parameters are per sub-cache. For example,
  // in case of multi-shard cache, when the 'iteration_func' returns 'false',
  // the invalidation at current shard stops and switches to the next
  // non-yet-processed shard, if any is present.
  //
  // The choice of the signature for the iteration functor is to allow for
  // effective purging of non-valid (e.g., expired) entries in caches with
  // the FIFO eviction policy (e.g., TTL caches).
  //
  // The first parameter of the functor is useful for short-circuiting
  // the invalidation process once some valid entries have been encountered.
  // For example, that's useful in case if the recency list has its entries
  // ordered in FIFO-like order (e.g., TTL cache with FIFO eviction policy),
  // so most-likely-invalid entries are in the very beginning of the list.
  // In the latter case, once a valid (e.g., not yet expired) entry is
  // encountered, there is no need to iterate any further: all the entries past
  // the first valid one in the recency list should be valid as well.
  //
  // The second parameter is useful when the validity criterion is fuzzy,
  // but there is a target number of entries to invalidate during each
  // invocation of the Invalidate() method or there is some logic that reads
  // the cache's metric(s) once the given number of entries have been evicted:
  // e.g., compare the result memory footprint of the cache against a threshold
  // to decide whether to continue invalidation of entries.
  //
  // Summing both parameters of the functor is useful when it's necessary to
  // limit the number of entries processed per one invocation of the
  // Invalidate() method. It makes sense in cases when a 'lazy' invalidation
  // process is run by a periodic task along with a significant amount of
  // concurrent requests to the cache, and the number of entries in the cache
  // is huge. Given the fact that in most cases it's necessary to guard
  // the access to the cache's recency list while iterating over it entries,
  // limiting the number of entries to process at once allows for better control
  // over the duration of the guarded/locked sections.
  typedef std::function<bool(size_t /* valid_entries_num */,
                             size_t /* invalid_entries_num */)>
      IterationFunc;

  // A helper function for 'validity_func' of the Invalidate() method:
  // invalidate all entries.
  static const ValidityFunc kInvalidateAllEntriesFunc;

  // A helper function for 'iteration_func' of the Invalidate() method:
  // examine all entries.
  static const IterationFunc kIterateOverAllEntriesFunc;

  // Control structure for the Invalidate() method. Combines the validity
  // and the iteration functors.
  struct InvalidationControl {
    // NOLINTNEXTLINE(google-explicit-constructor)
    InvalidationControl(ValidityFunc vfunctor = kInvalidateAllEntriesFunc,
                        IterationFunc ifunctor = kIterateOverAllEntriesFunc)
        : validity_func(std::move(vfunctor)),
          iteration_func(std::move(ifunctor)) {
    }
    const ValidityFunc validity_func;
    const IterationFunc iteration_func;
  };

 protected:
  // Release a mapping returned by a previous Lookup(), using raw handle.
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Free 'ptr', which must have been previously allocated using 'Allocate'.
  virtual void Free(PendingHandle* ptr) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Cache);
};

// Instantiate a cache of a particular 'policy' flavor with the specified 'capacity'
// and identifier 'id'.
Cache* NewCache(Cache::EvictionPolicy policy, size_t capacity, const std::string& id);

} // namespace impala
