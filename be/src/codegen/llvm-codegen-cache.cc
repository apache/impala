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

#include "codegen/llvm-codegen-cache.h"
#include "codegen/instruction-counter.h"
#include "codegen/mcjit-mem-mgr.h"
#include "util/hash-util.h"

using namespace std;
using strings::Substitute;

DEFINE_int32_hidden(codegen_cache_entry_bytes_charge_overhead, 0,
    "Value (in bytes) that is added to the memory charge of codegen cache entries. "
    "Used for testing.");

namespace impala {

// Maximum codegen cache entry size for the purposes of histogram sizing in stats
// collection.
// An entries is expected to be less than 128MB.
static constexpr int64_t STATS_MAX_CODEGEN_CACHE_ENTRY_SIZE = 128L << 20;

void CodeGenCache::EvictionCallback::EvictedEntry(Slice key, Slice value) {
  DCHECK(key.data() != nullptr);
  DCHECK(value.data() != nullptr);
  // Remove the execution engine of this entry from the global cache.
  const CodeGenCacheEntry* cache_entry =
      reinterpret_cast<const CodeGenCacheEntry*>(value.data());
  DCHECK(cache_entry != nullptr);
  cache_->RemoveEngine(cache_entry->cached_engine_pointer);
  LOG(INFO) << "Evict CodeGen Cache, key hash_code=" << CodeGenCacheKey::hash_code(key);
  // For statistics.
  if (codegen_cache_entries_evicted_ != nullptr) {
    codegen_cache_entries_evicted_->Increment(1);
  }
  if (codegen_cache_entries_in_use_ != nullptr) {
    codegen_cache_entries_in_use_->Increment(-1);
  }
  if (codegen_cache_entries_in_use_bytes_ != nullptr) {
    codegen_cache_entries_in_use_bytes_->Increment(-cache_entry->total_bytes_charge);
  }
}

void CodeGenCacheKeyConstructor::construct(
    string& key_content, CodeGenCacheKey* cache_key) {
  DCHECK(cache_key != nullptr);
  int length = 0;
  int len_hashcode = sizeof(CodeGenCacheKey::HashCode);
  // We use the same type for all the length of elements within the key.
  int len_general_size = sizeof(CodeGenCacheKey::ElementLengthType);
  int len_key_content = key_content.size();
  CodeGenCacheKey::HashCode hashcode;
  length += len_hashcode; // For hash value
  length += len_general_size << 1;
  length += len_key_content;
  string result;
  result.reserve(length);
  result.append(reinterpret_cast<char*>(&hashcode), len_hashcode);
  result.append(reinterpret_cast<char*>(&length), len_general_size);
  result.append(reinterpret_cast<char*>(&len_key_content), len_general_size);
  result.append(key_content.c_str(), len_key_content);
  DCHECK_GT(length, len_hashcode);
  MurmurHash3_x64_128(result.c_str() + len_hashcode, length - len_hashcode,
      CODEGEN_CACHE_HASH_SEED_CONST, hashcode.hash_code);
  result.replace(0, len_hashcode, reinterpret_cast<char*>(&hashcode), len_hashcode);
  cache_key->set_key(move(result));
}

CodeGenCache::CodeGenCache(MetricGroup* metrics)
  : is_closed_(false),
    codegen_cache_hits_(metrics->AddCounter("impala.codegen-cache.hits", 0)),
    codegen_cache_misses_(metrics->AddCounter("impala.codegen-cache.misses", 0)),
    codegen_cache_entries_evicted_(
        metrics->AddCounter("impala.codegen-cache.entries-evicted", 0)),
    codegen_cache_entries_in_use_(
        metrics->AddGauge("impala.codegen-cache.entries-in-use", 0)),
    codegen_cache_entries_in_use_bytes_(
        metrics->AddGauge("impala.codegen-cache.entries-in-use-bytes", 0)),
    codegen_cache_entry_size_stats_(metrics->RegisterMetric(
        new HistogramMetric(MetricDefs::Get("impala.codegen-cache.entry-sizes"),
            STATS_MAX_CODEGEN_CACHE_ENTRY_SIZE, 3))),
    evict_callback_(
        new CodeGenCache::EvictionCallback(this, codegen_cache_entries_evicted_,
            codegen_cache_entries_in_use_, codegen_cache_entries_in_use_bytes_)) {}

Status CodeGenCache::Init(int64_t capacity) {
  DCHECK(cache_ == nullptr);
  cache_.reset(NewCache(Cache::EvictionPolicy::LRU, capacity, "CodeGen_Cache"));
  return cache_->Init();
}

void CodeGenCache::ReleaseResources() {
  if (cache_ != nullptr) cache_.reset();
}

Status CodeGenCache::Lookup(const CodeGenCacheKey& cache_key,
    const TCodeGenCacheMode::type& mode, CodeGenCacheEntry* entry,
    shared_ptr<CodeGenObjectCache>* cached_engine) {
  DCHECK(!is_closed_);
  DCHECK(cache_ != nullptr);
  DCHECK(entry != nullptr);
  DCHECK(cached_engine != nullptr);
  // Use hash code and the total length as the key for optimal mode, because the whole
  // key could be very large, using optimal mode could improve the performance and save
  // memory consumption. However, it could lead to a collision, even though the chance
  // is very small, in that case, we may switch to normal mode for the query or disable
  // the codegen cache.
  Slice key;
  if (CodeGenCacheModeAnalyzer::is_optimal(mode)) {
    key = cache_key.optimal_key_slice();
  } else {
    key = cache_key.data_slice();
  }
  Cache::UniqueHandle handle(cache_->Lookup(key));
  if (handle.get() != nullptr) {
    const CodeGenCacheEntry* cached_entry =
        reinterpret_cast<const CodeGenCacheEntry*>(cache_->Value(handle).data());
    // Need to find the shared pointer of the engine from the cache before return,
    // because the shared pointer could be deleted in the eviction process.
    // If can't find it, treat it as cache missing, because the engine is needed
    // to look for jitted functions.
    if (LookupEngine(cached_entry->cached_engine_pointer, cached_engine)) {
      entry->Reset(cached_entry->cached_engine_pointer, cached_entry->num_functions,
          cached_entry->num_instructions, cached_entry->num_opt_functions,
          cached_entry->num_opt_instructions, cached_entry->function_names_hashcode,
          cached_entry->total_bytes_charge, cached_entry->opt_level);
      return Status::OK();
    }
  }
  entry->Reset();
  return Status::OK();
}

Status CodeGenCache::StoreInternal(const CodeGenCacheKey& cache_key,
    LlvmCodeGen* codegen, TCodeGenCacheMode::type mode,
    TCodeGenOptLevel::type opt_level) {
  // In normal mode, we will store the whole key content to the cache.
  // Otherwise, in optimal mode, we will only store the hash code and length of the key.
  Slice key;
  if (CodeGenCacheModeAnalyzer::is_optimal(mode)) {
    key = cache_key.optimal_key_slice();
  } else {
    key = cache_key.data_slice();
  }
  // Memory charge includes both key and entry size.
  int64_t mem_charge = codegen->engine_cache()->objSize() + key.size()
      + sizeof(CodeGenCacheEntry) + FLAGS_codegen_cache_entry_bytes_charge_overhead;
  Cache::UniquePendingHandle pending_handle(
      cache_->Allocate(key, sizeof(CodeGenCacheEntry), mem_charge));
  if (pending_handle == nullptr) {
    return Status(Substitute("Couldn't allocate handle for codegen cache entry,"
                             " size: '$0'",
        mem_charge));
  }
  CodeGenCacheEntry* cache_entry =
      reinterpret_cast<CodeGenCacheEntry*>(cache_->MutableValue(&pending_handle));
  cache_entry->Reset(codegen->engine_cache(), codegen->num_functions_->value(),
      codegen->num_instructions_->value(), codegen->num_opt_functions_->value(),
      codegen->num_opt_instructions_->value(), codegen->function_names_hashcode_,
      mem_charge, opt_level);
  StoreEngine(codegen);
  /// It is thread-safe, but could override the existing entry with the same key.
  Cache::UniqueHandle cache_handle =
      cache_->Insert(move(pending_handle), evict_callback_.get());
  if (cache_handle == nullptr) {
    RemoveEngine(codegen->engine_cache());
    return Status(Substitute("Couldn't insert codegen cache entry,"
                             " hash code:'$0', size: '$1'",
        cache_key.hash_code().str(), mem_charge));
  }
  codegen_cache_entries_in_use_->Increment(1);
  codegen_cache_entries_in_use_bytes_->Increment(mem_charge);
  codegen_cache_entry_size_stats_->Update(mem_charge);
  return Status::OK();
}

Status CodeGenCache::Store(const CodeGenCacheKey& cache_key, LlvmCodeGen* codegen,
    TCodeGenCacheMode::type mode, TCodeGenOptLevel::type opt_level) {
  DCHECK(!is_closed_);
  DCHECK(cache_ != nullptr);
  DCHECK(codegen != nullptr);
  Status status = Status::OK();
  pair<unordered_set<CodeGenCacheKey::HashCode>::iterator, bool> key_to_insert_it;
  {
    // The Cache::Insert() suggests the caller to avoid multiple handles for the same
    // key to insert because it can be inefficient to keep the entry from being evicted.
    // So use the keys_to_insert_ set to reduce the chance of multiple insertion of the
    // same cache entry.
    // Before insertion, we will try to insert the hash code of the key to the set, if
    // succeeds, the thread is allowed to do the insertion, and is responsible to erase
    // the hash code in the set when insertion finishes.
    // Otherwise, if the thread fails to insert the hash code, that means one other thread
    // is doing the insertion on the same key, in that case, the current thread would give
    // up the task and return with an okay status, because we assume the other thread
    // would get the cache entry in. Even that thread fails, the system won't hurt without
    // only one cache entry, and the cache entry can be inserted again next time.
    lock_guard<mutex> lock(to_insert_set_lock_);
    key_to_insert_it = keys_to_insert_.insert(cache_key.hash_code());
    // If hash code exists, return an okay immediately.
    if (!key_to_insert_it.second) return status;
  }
  status = StoreInternal(cache_key, codegen, mode, opt_level);
  // Remove the hash code of the key from the to_insert_keys set.
  lock_guard<mutex> lock(to_insert_set_lock_);
  keys_to_insert_.erase(key_to_insert_it.first);
  return status;
}

void CodeGenCache::StoreEngine(LlvmCodeGen* codegen) {
  DCHECK(codegen != nullptr);
  lock_guard<mutex> lock(cached_engines_lock_);
  cached_engines_.emplace(codegen->engine_cache_.get(), codegen->engine_cache_);
}

bool CodeGenCache::LookupEngine(const CodeGenObjectCache* cached_engine_raw_ptr,
    shared_ptr<CodeGenObjectCache>* cached_engine) {
  DCHECK(cached_engine_raw_ptr != nullptr);
  lock_guard<mutex> lock(cached_engines_lock_);
  auto engine_it = cached_engines_.find(cached_engine_raw_ptr);
  if (engine_it == cached_engines_.end()) return false;
  if (cached_engine != nullptr) {
    *cached_engine = engine_it->second;
  }
  return true;
}

void CodeGenCache::RemoveEngine(const CodeGenObjectCache* cached_engine_raw_ptr) {
  DCHECK(cached_engine_raw_ptr != nullptr);
  lock_guard<mutex> lock(cached_engines_lock_);
  cached_engines_.erase(cached_engine_raw_ptr);
}

} // namespace impala
