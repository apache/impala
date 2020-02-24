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
    case Cache::EvictionPolicy::LIRS:
      return "lirs";
    default:
      LOG(FATAL) << "unexpected cache eviction policy: " << static_cast<int>(p);
  }
  return "unknown";
}

CacheShard* NewCacheShard(Cache::EvictionPolicy policy, kudu::MemTracker* mem_tracker,
    size_t capacity) {
  switch (policy) {
    case Cache::EvictionPolicy::FIFO:
      return NewCacheShardInt<Cache::EvictionPolicy::FIFO>(mem_tracker, capacity);
    case Cache::EvictionPolicy::LRU:
      return NewCacheShardInt<Cache::EvictionPolicy::LRU>(mem_tracker, capacity);
    case Cache::EvictionPolicy::LIRS:
      return NewCacheShardInt<Cache::EvictionPolicy::LIRS>(mem_tracker, capacity);
    default:
      LOG(FATAL) << "unexpected cache eviction policy: " << static_cast<int>(policy);
  }
}

Cache* NewCache(Cache::EvictionPolicy policy, size_t capacity, const std::string& id) {
  return new ShardedCache(policy, capacity, id);
}

}  // namespace impala
