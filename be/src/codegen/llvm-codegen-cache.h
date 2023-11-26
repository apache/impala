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

#pragma once

#include "codegen/llvm-codegen.h"

#include <unistd.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "kudu/util/slice.h"
#include "thirdparty/datasketches/MurmurHash3.h"
#include "util/cache/cache.h"
#include "util/histogram-metric.h"
#include "util/metrics.h"

namespace impala {

/// The key to a CodeGen Cache entry.
/// It contains a hash code at the first of sizeof(struct HashCode) bytes. If the
/// cache is in NORMAL mode, the content of the key will be after the hash code,
/// which could be a combination of several keys, otherwise, in OPTIMAL mode, the
/// CodeGenCacheKey to be stored to the cache would only contain a hash code and
/// the total length to reduce the memory consumption.
/// For a key in the NORMAL mode, it would be like:
/// Hash Code| Total Length| Length of Key 1| Data of Key 1| Length of Key 2
/// | Data of Key 2| ... | Length of Key N| Data of Key N.
/// The key is constructed by CodeGenCacheKeyConstructor.
/// Functions are NOT thread-safe.
class CodeGenCacheKey {
 public:
  /// The type of a hashcode for the codegen cache key.
  struct HashCode {
    HashCode() { memset(&hash_code, 0, sizeof(HashState)); }
    // This function is used by unordered_set to compare elements of HashCode.
    bool operator==(const HashCode& h) const {
      return (this->hash_code.h1 == h.hash_code.h1)
          && (this->hash_code.h2 == h.hash_code.h2);
    }
    friend std::ostream& operator<<(std::ostream& o, const HashCode& h) {
      return o << std::hex << std::setfill('0') << std::setw(16) << h.hash_code.h1 << ":"
               << std::setw(16) << h.hash_code.h2;
    }
    string str() {
      std::stringstream out;
      out << *this;
      return out.str();
    }
    HashState hash_code;
  };

  /// Used as the hash function in unordered_set for struct HashCode.
  /// It is required by the unordered_set to generate a hash code for a
  /// customized structure.
  struct HashCodeHash {
    uint64_t operator()(const HashCode& h) const {
      // Return the first 64bits for the hash.
      return h.hash_code.h1;
    }
  };

  /// The type of the length of a general element in the codegen cache key.
  typedef int ElementLengthType;

  /// Construct an empty key.
  CodeGenCacheKey() {}

  /// Construct the key from a string. Uses move to fasten the process.
  CodeGenCacheKey(string key) : key_(std::move(key)) {}

  /// Set the data of the key. Uses swap to fasten the process.
  void set_key(string key) { key_.swap(key); }

  /// Return the hash code of the key.
  HashCode hash_code() const {
    DCHECK(!key_.empty());
    DCHECK(key_.length() >= sizeof(HashCode));
    return *(const HashCode*)key_.c_str();
  }

  /// Return the slice of the hash code of the key.
  Slice hash_code_slice() const {
    DCHECK_LE(sizeof(HashCode), key_.size());
    return Slice(key_.c_str(), sizeof(HashCode));
  }

  /// Return the size of a key in optimal mode.
  static constexpr size_t OptimalKeySize = sizeof(HashCode) + sizeof(ElementLengthType);

  /// Return the slice of key in optimal mode, which is the hash code plus the total
  /// length of a full key.
  Slice optimal_key_slice() const {
    DCHECK_LE(OptimalKeySize, key_.size());
    return Slice(key_.c_str(), OptimalKeySize);
  }

  /// Return the hash code from a key of type Slice.
  static HashCode hash_code(Slice key) {
    DCHECK(!key.empty());
    DCHECK_LE(sizeof(HashCode), key.size());
    return *(const HashCode*)key.data();
    ;
  }

  /// Return the data of the key.
  string& data() { return key_; }
  Slice data_slice() const { return Slice(key_.c_str(), key_.size()); }

  /// Return if the key is empty.
  bool empty() const { return key_.empty(); }

 private:
  /// The data of the key.
  std::string key_;
};

/// The class helps to construct a CodeGenCacheKey.
class CodeGenCacheKeyConstructor {
 public:
  /// Construct a CodeGenCacheKey from a string.
  /// It could be in future to construct from a list of string keys.
  static void construct(std::string&, CodeGenCacheKey*);
  /// An arbitrary constant used to seed the hash.
  static constexpr uint64_t CODEGEN_CACHE_HASH_SEED_CONST = 0x6b8b4567327b23c6;
};

/// The class helps to analyze the codegen cache mode.
class CodeGenCacheModeAnalyzer {
 public:
  // The mask should follow the definition of TCodeGenCacheMode.
  // The lowest byte of the TCodeGenCacheMode is used to define the type NORMAL
  // or OPTIMAL.
  // The nineth bit (bit 8) is used to define whether it is a debug mode. If it is 1,
  // then it is a debug mode.
  static const uint64_t CODEGEN_CACHE_MODE_MASK = 0xFF;
  static const uint64_t CODEGEN_CACHE_MODE_BITS = 8;
  static bool is_optimal(const TCodeGenCacheMode::type& mode) {
    return (mode & CODEGEN_CACHE_MODE_MASK) == TCodeGenCacheMode::OPTIMAL;
  }
  static bool is_debug(const TCodeGenCacheMode::type& mode) {
    return (mode >> CODEGEN_CACHE_MODE_BITS) != 0;
  }
};

struct CodeGenCacheEntry {
  CodeGenCacheEntry() : cached_engine_pointer(nullptr) {}
  // When Empty, no guarantees are made about the content of other fields.
  bool Empty() { return cached_engine_pointer == nullptr; }
  void Reset() { cached_engine_pointer = nullptr; }
  void Reset(CodeGenObjectCache* cached_engine_ptr, int64_t num_funcs,
      int64_t num_instrucs, int64_t num_opt_funcs, int64_t num_opt_instrucs,
      uint64_t names_hashcode, int64_t charge, TCodeGenOptLevel::type opt) {
    cached_engine_pointer = cached_engine_ptr;
    num_functions = num_funcs;
    num_instructions = num_instrucs;
    num_opt_functions = num_opt_funcs;
    num_opt_instructions = num_opt_instrucs;
    function_names_hashcode = names_hashcode;
    total_bytes_charge = charge;
    opt_level = opt;
  }
  CodeGenObjectCache* cached_engine_pointer;
  /// Number of functions before optimization.
  int64_t num_functions;
  /// Number of instructions before optimization.
  int64_t num_instructions;
  /// Number of functions after optimization.
  int64_t num_opt_functions;
  /// Number of instructions after optimization.
  int64_t num_opt_instructions;
  /// The hashcode of function names in the entry.
  uint64_t function_names_hashcode;
  /// Bytes charge including the key and the entry.
  int64_t total_bytes_charge;
  /// CodeGen optimization level used to generate this entry.
  TCodeGenOptLevel::type opt_level;
};

/// Each CodeGenCache is supposed to be a singleton in the daemon, manages the codegen
/// cache entries, including providing the function of entry storage and look up, and
/// entry eviction if capacity limit is met.
class CodeGenCache {
 public:
  CodeGenCache(MetricGroup*);
  ~CodeGenCache() {
    is_closed_ = true;
    ReleaseResources();
  }

  /// Initilization for the codegen cache, including cache and metrics allocation.
  Status Init(int64_t capacity);

  /// Release the resources that occupied by the codegen cache before destruction.
  void ReleaseResources();

  /// Lookup the specific cache key for the cache entry. If the key doesn't exist, the
  /// entry will be reset to empty.
  /// Return Status::Okay unless there is any internal error to throw.
  Status Lookup(const CodeGenCacheKey& key, const TCodeGenCacheMode::type& mode,
      CodeGenCacheEntry* entry, std::shared_ptr<CodeGenObjectCache>* cached_engine);
  bool Lookup(const CodeGenCacheKey& key, const TCodeGenCacheMode::type& mode);

  /// Store the cache entry with the specific cache key.
  Status Store(const CodeGenCacheKey& key, LlvmCodeGen* codegen,
      TCodeGenCacheMode::type mode, TCodeGenOptLevel::type opt_level);

  /// Store the shared pointer of CodeGenObjectCache to keep the compiled module cache
  /// alive.
  void StoreEngine(LlvmCodeGen* codegen);

  /// Look up the shared pointer of the CodeGenObjectCache by its raw pointer.
  /// If found, return true. Ohterwise, return false.
  bool LookupEngine(const CodeGenObjectCache* cached_engine_raw_ptr,
      std::shared_ptr<CodeGenObjectCache>* cached_engine = nullptr);

  /// Remove the shared pointer of CodeGenObjectCache from the cache by its raw pointer
  /// address.
  void RemoveEngine(const CodeGenObjectCache* cached_engine_raw_ptr);

  /// Increment a hit count or miss count.
  void IncHitOrMissCount(bool hit) {
    DCHECK(codegen_cache_hits_ && codegen_cache_misses_);
    if (hit) {
      codegen_cache_hits_->Increment(1);
    } else {
      codegen_cache_misses_->Increment(1);
    }
  }

  /// EvictionCallback for the codegen cache.
  class EvictionCallback : public Cache::EvictionCallback {
   public:
    EvictionCallback(CodeGenCache* cache, IntCounter* entries_evicted,
        IntGauge* entries_in_use, IntGauge* entries_in_use_bytes)
      : cache_(cache),
        codegen_cache_entries_evicted_(entries_evicted),
        codegen_cache_entries_in_use_(entries_in_use),
        codegen_cache_entries_in_use_bytes_(entries_in_use_bytes) {}
    virtual void EvictedEntry(Slice key, Slice value) override;

   private:
    CodeGenCache* cache_;
    /// Metrics for the codegen cache in the process level.
    IntCounter* codegen_cache_entries_evicted_;
    IntGauge* codegen_cache_entries_in_use_;
    IntGauge* codegen_cache_entries_in_use_bytes_;
  };

 private:
  friend class LlvmCodeGenCacheTest;

  /// Helper function to store the entry to the cache.
  Status StoreInternal(const CodeGenCacheKey& key, LlvmCodeGen* codegen,
      TCodeGenCacheMode::type mode, TCodeGenOptLevel::type opt_level);

  /// Indicate if the cache is closed. If is closed, no call is allowed for any functions
  /// other than ReleaseResources().
  bool is_closed_;

  /// The instance of the cache.
  std::unique_ptr<Cache> cache_;

  /// Metrics for the codegen cache in the process level.
  IntCounter* codegen_cache_hits_;
  IntCounter* codegen_cache_misses_;
  IntCounter* codegen_cache_entries_evicted_;
  IntGauge* codegen_cache_entries_in_use_;
  IntGauge* codegen_cache_entries_in_use_bytes_;

  /// Statistics for the buffer sizes allocated from the system allocator.
  HistogramMetric* codegen_cache_entry_size_stats_;

  /// Eviction Callback function.
  std::unique_ptr<CodeGenCache::EvictionCallback> evict_callback_;

  /// Protects to the to insert hash set below.
  std::mutex to_insert_set_lock_;

  /// The keys of codegen entries that are going to insert to the system cache.
  /// The purpose of it is to prevent the same key simultaneously to be inserted.
  std::unordered_set<CodeGenCacheKey::HashCode, CodeGenCacheKey::HashCodeHash>
      keys_to_insert_;

  /// Protects to the map of cached engines.
  std::mutex cached_engines_lock_;

  /// Stores shared pointers to CodeGenObjectCaches to keep the jitted functions
  /// alive. The shared pointer entries could be removed when the cache entry is evicted
  /// or when the whole cache is destructed.
  std::unordered_map<const CodeGenObjectCache*, std::shared_ptr<CodeGenObjectCache>>
      cached_engines_;
};
} // namespace impala
