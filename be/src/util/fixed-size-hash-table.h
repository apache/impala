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


#ifndef IMPALA_UTIL_FAST_HASH_TABLE_H
#define IMPALA_UTIL_FAST_HASH_TABLE_H

#include <algorithm>

#include "common/status.h"
#include "util/bit-util.h"
#include "util/hash-util.h"

namespace impala {

/// Fixed-size hash table that resolves collisions with linear probing and does not
/// support element removal. Clients of this class must size the table appropriately.
/// This allows very lightweight operations at the cost of generality.
/// The key type K must be a pointer and NULL keys cannot be used beause NULL is used
/// as a sentinel value internally.
/// The capacity should be chosen to be significantly larger (e.g. 2x) the maximum number
/// of elements that will be inserted into the table. DO NOT USE THIS TABLE if there is
/// any chance that you will insert a number of elements even close to the capacity. If
/// the number of inserted elements approaches the capacity, performance will degrade
/// significantly. If the size reaches capacity, an infinite loop will result.
/// The capacity is rounded up to the nearest power of two internally.
/// TODO: could generalize to support specifying null value as template arg.
template <typename K, typename V>
class FixedSizeHashTable {
 public:
  FixedSizeHashTable() : tbl_(NULL), capacity_(0), size_(0), hash_seed_(0) {}

  /// Initialize hash table to the next power of two greater than capacity.
  Status Init(uint32_t min_capacity, uint32_t hash_seed) {
    DCHECK_GT(min_capacity, 0);
    // Capacity cannot be greater than largest uint32_t power of two.
    capacity_ = static_cast<uint32_t>(std::min(static_cast<int64_t>(1) << 31,
        BitUtil::RoundUpToPowerOfTwo(min_capacity)));
    DCHECK_EQ(capacity_, BitUtil::RoundUpToPowerOfTwo(capacity_));
    if (tbl_ != NULL) free(tbl_);
    int64_t tbl_byte_size = capacity_ * sizeof(Entry);
    tbl_ = reinterpret_cast<Entry*>(malloc(tbl_byte_size));
    if (tbl_ == NULL) return Status(TErrorCode::MEM_ALLOC_FAILED, tbl_byte_size);
    hash_seed_ = hash_seed;
    Clear();
    return Status::OK();
  }

  ~FixedSizeHashTable() {
    if (tbl_ != NULL) {
      free(tbl_);
      tbl_ = NULL;
    }
  }

  /// Clear all entries from table. Table must be initialized.
  void Clear() {
    DCHECK(tbl_ != NULL);
    memset(tbl_, 0, capacity_ * sizeof(Entry));
    size_ = 0;
  }

  /// Get an entry from table, return true and set val if found.
  /// key should not be null.
  bool Find (const K key, V* val) const {
    Entry* entry = FindEntry(key);
    if (entry->key == NULL) return false;
    *val = entry->val;
    return true;
  }

  /// Insert entry, overwriting previous entry if it already exists.
  /// key should not be null.
  void Insert(const K key, const V& val) {
    DCHECK_LT(size_, capacity_);
    Entry* entry = FindEntry(key);
    size_ += entry->key == NULL;
    entry->key = key;
    entry->val = val;
  }

  /// Insert entry if entry with same key not present. key should not be null.
  /// Return true if insert occurred.
  bool InsertIfNotPresent(const K key, const V& val) {
    DCHECK_LT(size_, capacity_);
    Entry* entry = FindEntry(key);
    if (entry->key != NULL) return false;
    entry->key = key;
    entry->val = val;
    ++size_;
    return true;
  }

  /// Try to find entry, and insert default_val if not present. Return a pointer to the
  /// value. key should not be null.
  V* FindOrInsert(const K key, const V& default_val) {
    DCHECK_LT(size_, capacity_);
    Entry* entry = FindEntry(key);
    if (entry->key == NULL) {
      entry->key = key;
      entry->val = default_val;
      ++size_;
    }
    return &entry->val;
  }

  uint32_t size() const { return size_; }

  uint32_t capacity() const { return capacity_; }

 private:
  struct Entry {
    K key;
    V val;
  };

  /// Table with capacity_ entries allocated with malloc.
  Entry* tbl_;

  /// Capacity, which must be a power of two.
  uint32_t capacity_;

  /// Number of elements in table.
  uint32_t size_;

  /// Seed to use for hash computation.
  uint32_t hash_seed_;

  Entry* FindEntry (K key) const {
    DCHECK(tbl_ != NULL);
    DCHECK(key != NULL); // NULL key is already used to mark empty entries.
    uint32_t hash = HashUtil::Hash(&key, sizeof(key), hash_seed_);
    // Mod can be computed efficiently with bitmask since capacity is a power of two.
    uint32_t mask = capacity_ - 1;
    uint32_t bucket = hash & mask;
    // Infinite loop if size_ >= capacity_. Users of class must be careful.
    DCHECK_LT(size_, capacity_);
    Entry* entry;
    do {
      entry = &tbl_[bucket];
      bucket = (bucket + 1) & mask;
    } while (entry->key != NULL && entry->key != key);
    return entry;
  }
};

}

#endif
