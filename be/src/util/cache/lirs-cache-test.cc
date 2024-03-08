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
#include "util/cache/cache-test.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

DECLARE_double(lirs_unprotected_percentage);
DECLARE_double(lirs_tombstone_multiple);

namespace impala {

class LIRSCacheTest : public CacheBaseTest {
 public:
  LIRSCacheTest()
    : CacheBaseTest(100) {
    FLAGS_lirs_unprotected_percentage = 5.0;
    FLAGS_lirs_tombstone_multiple = 2.0;
  }

  void SetUp() override {
    SetupWithParameters(Cache::EvictionPolicy::LIRS, ShardingPolicy::SingleShard);
  }

 protected:
  // This fills the cache with 100 elements (0-99) and verifies that there were no
  // evictions. Entries are inserted into the protected space until it is full
  // (95 spots, so 0-94), then they are inserted as unprotected (5 spots, so 95-99).
  void FillCache() {
    for (int i = 0; i < 100; ++i) {
      ASSERT_TRUE(Insert(i, i));
    }
    ASSERT_EQ(evicted_keys_.size(), 0);
    ASSERT_EQ(evicted_values_.size(), 0);
  }

  // This fills the cache and keeps adding elements until it reaches the limit on the
  // number of tombstones. This will add 300 elements. 100 remain in the cache at the
  // end along with 200 tombstones. This also clears the evicted_key_ and evicted_values_
  // to provide a clean slate for tests.
  void FillCacheToTombstoneLimit() {
    for (int i = 0; i < 300; ++i) {
      ASSERT_TRUE(Insert(i, i));
    }
    // Items 0-94 remain protected.
    // Items 95-294 were evicted and are tombstones.
    // Items 295-299 are unprotected.
    for (int i = 0; i < 200; ++i) {
      ASSERT_EQ(evicted_keys_[i], i+95);
      ASSERT_EQ(evicted_values_[i], i+95);
    }
    evicted_keys_.clear();
    evicted_values_.clear();
  }

  // This forces all entries to be evicted. It repeatedly references entirely new
  // elements twice in a row, forcing them to become protected. The preexisting
  // elements should be evicted starting with the unprotected in FIFO order followed by
  // the protected in FIFO order.
  void FlushCache() {
    for (int i = 0; i < 100; ++i) {
      // Insert as unprotected
      ASSERT_TRUE(Insert(1000 + i, 1000 + i));
      // Lookup moves to protected
      ASSERT_EQ(Lookup(1000 + i), 1000 + i);
    }
  }
};

// This tests a very simple case: insert entries, then flush the cache and verify
// entries are evicted in the appropriate order.
TEST_F(LIRSCacheTest, BasicEvictionOrdering) {
  FillCache();
  FlushCache();

  // There were 5 unprotected elements (95-99). They should be the first to be evicted.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // There were 95 protected elements (0-94). They should be evicted in order.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// Lookup operations can be tagged as NO_UPDATE, in which case, nothing should change
// priority. Verify that adding NO_UPDATE Lookups to the basic case does not change
// the eviction order because nothing moves.
TEST_F(LIRSCacheTest, LookupNoUpdate) {
  FillCache();

  // Do a NO_UPDATE lookup on an unprotected element (which otherwise would become
  // protected)
  Lookup(97, Cache::NO_UPDATE);

  // Do a NO_UPDATE lookup on a protected element (which otherwise would move in the
  // recency queue).
  Lookup(55, Cache::NO_UPDATE);

  FlushCache();

  // There were 5 unprotected elements (95-99). They should be the first to be evicted.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // There were 95 protected elements (0-94). They should be evicted in order.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// Test that Allocate() rejects anything larger than the protected capacity (which is 95)
TEST_F(LIRSCacheTest, RejectLarge) {
  // Insert() returns false if Allocate() fails.
  ASSERT_FALSE(Insert(100, 100, 96));
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);

  // Allocate() does not reject something of exactly the protected capacity (95)
  ASSERT_TRUE(Insert(100, 100, 95));
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);
}

// This tests that inserting a single large element can evict all of the UNPROTECTED
// elements, along with itself.
TEST_F(LIRSCacheTest, LargeInsertUnprotectedEvict) {
  FillCache();
  ASSERT_FALSE(Insert(100, 100, 6));
  // All 5 UNPROTECTED got evicted, along with the element being inserted.
  ASSERT_EQ(evicted_keys_.size(), 6);
  ASSERT_EQ(evicted_values_.size(), 6);
  for (int i = 0; i < 6; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // Only the protected remain, and they are all in order
  ASSERT_EQ(evicted_keys_.size(), 95);
  ASSERT_EQ(evicted_values_.size(), 95);
  for (int i = 0; i < 95; ++i) {
    ASSERT_EQ(evicted_keys_[i], i);
    ASSERT_EQ(evicted_values_[i], i);
  }
}

// This tests that updating a single large element evicts all UNPROTECTED elements
TEST_F(LIRSCacheTest, LargeUpdateUnprotectedEvict) {
  FillCache();
  UpdateCharge(99, 6);
  // All 5 UNPROTECTED got evicted, along with the element being updated.
  ASSERT_EQ(evicted_keys_.size(), 5);
  ASSERT_EQ(evicted_values_.size(), 5);
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // Only the protected remain, and they are all in order
  ASSERT_EQ(evicted_keys_.size(), 95);
  ASSERT_EQ(evicted_values_.size(), 95);
  for (int i = 0; i < 95; ++i) {
    ASSERT_EQ(evicted_keys_[i], i);
    ASSERT_EQ(evicted_values_[i], i);
  }
}

// This tests that a PROTECTED element that is larger than the unprotected capacity
// will evict everything including itself when it transitions to UNPROTECTED.
TEST_F(LIRSCacheTest, LargeProtectedToUnprotectedEvict) {
  // Insert PROTECTED element that is larger than the unprotected capacity
  ASSERT_TRUE(Insert(0, 0, 95));
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);

  // Insert 5 elements, which will be UNPROTECTED
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(Insert(100+i, 100+i));
  }
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);

  // Looking up an UNPROTECTED element will transition it to PROTECTED, evicting
  // the very large PROTECTED element. That will evict all four of the UNPROTECTED
  // elements along with itself.
  ASSERT_EQ(Lookup(104), 104);
  ASSERT_EQ(evicted_keys_.size(), 5);
  ASSERT_EQ(evicted_values_.size(), 5);

  // UNPROTECTED elements were evicted first
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(evicted_keys_[i], 100+i);
    ASSERT_EQ(evicted_values_[i], 100+i);
  }
  // Large formerly PROTECTED element is evicted last
  ASSERT_EQ(evicted_keys_[4], 0);
  ASSERT_EQ(evicted_values_[4], 0);
}

// Large unprotected entries can transition directly to being a TOMBSTONE on Insert().
// This test verifies that this TOMBSTONE entry (which is larger than the unprotected
// capacity) can be promoted to be PROTECTED if there is another Insert().
TEST_F(LIRSCacheTest, LargeTombstone) {
  // One protected element
  ASSERT_TRUE(Insert(0, 0, 95));
  ASSERT_EQ(Lookup(0), 0);
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);

  // Large unprotected element is immediately tombstone
  ASSERT_FALSE(Insert(1, 1, 95));
  ASSERT_EQ(evicted_keys_.size(), 1);
  ASSERT_EQ(evicted_keys_[0], 1);
  ASSERT_EQ(evicted_values_[0], 1);
  ASSERT_EQ(Lookup(1), -1);
  evicted_keys_.clear();
  evicted_values_.clear();

  // Insert of tombstone turns it protected, evicting the current protected key
  ASSERT_TRUE(Insert(1, 1, 95));
  ASSERT_EQ(evicted_keys_.size(), 1);
  ASSERT_EQ(evicted_keys_[0], 0);
  ASSERT_EQ(evicted_values_[0], 0);
  ASSERT_EQ(Lookup(1), 1);
  ASSERT_EQ(Lookup(0), -1);
}

// A client could hold a handle for an entry that gets evicted and becomes a TOMBSTONE
// entry. This test verifies that if they call UpdateCharge() on a TOMBSTONE entry,
// then nothing happens.
TEST_F(LIRSCacheTest, UpdateChargeTombstone) {
  FillCache();

  // Get a handle to an entry
  auto handle(cache_->Lookup(EncodeInt(1), Cache::NO_UPDATE));

  // Flush the cache
  FlushCache();

  // Verify our key has been evicted. It can't be found via Lookup().
  ASSERT_EQ(Lookup(1, Cache::NO_UPDATE), -1);

  evicted_keys_.clear();
  evicted_values_.clear();

  // Try to update the charge for our tombstone entry
  cache_->UpdateCharge(handle, cache_->MaxCharge());
  // The charge doesn't actually get updated
  ASSERT_EQ(cache_->Charge(handle), 1);

  // Nothing gets evicted.
  ASSERT_EQ(evicted_keys_.size(), 0);
  ASSERT_EQ(evicted_values_.size(), 0);
}

// This tests the behavior of insert when there is already an unprotected element with
// the same key. It should replace the existing value, but the new element should
// continue to be unprotected.
TEST_F(LIRSCacheTest, InsertExistingUnprotected) {
  FillCache();

  // Replace an unprotected key with a new value
  Insert(95, 1095);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 95);
  ASSERT_EQ(evicted_values_[0], 95);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // The only thing we guarantee is that key 95 is still unprotected. None of the other
  // unprotected elements should have moved around, but the ordering is not particularly
  // important, so this only verifies that the values are still around.
  for (int i = 0; i < 5; ++i) {
    ASSERT_LT(evicted_keys_[i], 100);
    ASSERT_GE(evicted_keys_[i], 95);
    if (evicted_keys_[i] == 95) {
      ASSERT_EQ(evicted_values_[i], 1095);
    } else {
      ASSERT_EQ(evicted_values_[i], evicted_keys_[i]);
    }
  }
  // There were 95 protected elements (0-94). They are not impacted, and they are still
  // evicted in order.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// This is the same as InsertExistingUnprotected, except that it is verifying that
// replacing an existing protected key will remain protected.
TEST_F(LIRSCacheTest, InsertExistingProtected) {
  FillCache();

  // Replace a protected key with a new value
  Insert(25, 1025);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 25);
  ASSERT_EQ(evicted_values_[0], 25);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // There were 5 unprotected elements (95-99). They are not impacted by changes in
  // the protected elements.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // The only thing we guarantee is that key 25 is still protected. None of the other
  // protected elements moved around, but the exact ordering is not specified.
  for (int i = 5; i < 100; ++i) {
    ASSERT_LT(evicted_keys_[i], 95);
    ASSERT_GE(evicted_keys_[i], 0);
    if (evicted_keys_[i] == 25) {
      ASSERT_EQ(evicted_values_[i], 1025);
    } else {
      ASSERT_EQ(evicted_values_[i], evicted_keys_[i]);
    }
  }
}

// This is the same as InsertExistingProtected, except that it is verifying that
// replacing an existing protected key that is the last entry on the recency
// list will trim the recency list.
TEST_F(LIRSCacheTest, InsertExistingProtectedNeedsTrim) {
  FillCache();

  // Lookup every protected value except #25
  // This doesn't change which keys are protected vs unprotected, but
  // it changes the order of the elements on the recency list.
  // 25 will be the oldest element, then there will be all of the
  // unprotected elements, then all the other protected elements.
  for (int i = 0; i < 95; ++i) {
    if (i != 25) {
      ASSERT_EQ(Lookup(i), i);
    }
  }

  // Replace this last protected key with a new value. When this entry is inserted,
  // it will remove the old protected key. This makes the unprotected entries the
  // last on the list, so they will be trimmed off of the recency list.
  Insert(25, 1025);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 25);
  ASSERT_EQ(evicted_values_[0], 25);
  evicted_keys_.clear();
  evicted_values_.clear();

  // If the recency list wasn't trimmed appropriately, then this would hit an assert.
  FlushCache();

  // There were 5 unprotected elements (95-99). They were not impacted by changes in
  // the protected elements.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // The only thing we guarantee is that key 25 is still protected. None of the other
  // protected elements moved around, but the exact ordering is not specified.
  for (int i = 5; i < 100; ++i) {
    ASSERT_LT(evicted_keys_[i], 95);
    ASSERT_GE(evicted_keys_[i], 0);
    if (evicted_keys_[i] == 25) {
      ASSERT_EQ(evicted_values_[i], 1025);
    } else {
      ASSERT_EQ(evicted_values_[i], evicted_keys_[i]);
    }
  }
}

// This tests the behavior of UpdateCharge on an UNPROTECTED element. It should update
// the change, but the element should continue to be unprotected.
TEST_F(LIRSCacheTest, UpdateExistingUnprotected) {
  FillCache();

  // Increase the charge
  UpdateCharge(96, 2);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 95);
  ASSERT_EQ(evicted_values_[0], 95);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // The only thing we guarantee is that key 96 is still unprotected. None of the other
  // unprotected elements should have moved around, but the ordering is not particularly
  // important, so this only verifies that the values are still around.
  for (int i = 0; i < 4; ++i) {
    ASSERT_LT(evicted_keys_[i], 100);
    ASSERT_GE(evicted_keys_[i], 96);
  }
  // There were 95 protected elements (0-94). They are not impacted, and they are still
  // evicted in order.
  for (int i = 4; i < 99; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-4);
    ASSERT_EQ(evicted_values_[i], i-4);
  }
}

// This is the same as UpdateExistingUnprotected, except that it is verifying that
// updating the charge on a protected key remains protected, and evictions cascade.
TEST_F(LIRSCacheTest, UpdateExistingProtected) {
  FillCache();

  // Increase the charge on a protected key, pushing a protected key to unprotected and
  // evicting an unprotected key.
  UpdateCharge(25, 2);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 95);
  ASSERT_EQ(evicted_values_[0], 95);
  evicted_keys_.clear();
  evicted_values_.clear();

  // Evict all unprotected keys.
  ASSERT_FALSE(Insert(100, 100, 6));
  ASSERT_EQ(6, evicted_keys_.size());
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(evicted_keys_[i], 96+i);
    ASSERT_EQ(evicted_values_[i], 96+i);
  }
  // The last evicted element before the Insert was protected.
  ASSERT_EQ(evicted_keys_[4], 0);
  ASSERT_EQ(evicted_values_[4], 0);
  ASSERT_EQ(evicted_keys_[5], 100);
  ASSERT_EQ(evicted_values_[5], 100);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // None of the other protected elements moved around, but the exact ordering is not
  // specified.
  for (int i = 0; i < 94; ++i) {
    ASSERT_LT(evicted_keys_[i], 95);
    ASSERT_GE(evicted_keys_[i], 0);
  }
}

// This tests the behavior of lookup of an unprotected key that is more recent than
// the oldest protected key (i.e. it should be promoted to be protected).
TEST_F(LIRSCacheTest, UnprotectedToProtected) {
  FillCache();

  // If we lookup 95, it will move from unprotected to protected.
  ASSERT_EQ(Lookup(95), 95);
  ASSERT_EQ(0, evicted_keys_.size());

  FlushCache();

  // The 5 unprotected elements are 96, 97, 98, 99, 0 (pushed out of protected)

  // There were 5 unprotected elements (95-99). They are not impacted by changes in
  // the protected elements.
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(evicted_keys_[i], 96+i);
    ASSERT_EQ(evicted_values_[i], 96+i);
  }
  ASSERT_EQ(evicted_keys_[4], 0);
  ASSERT_EQ(evicted_values_[4], 0);

  // 0 was pushed out of protected, and 95 was added (at the tail of the recency queue).
  // So, this is just one offset from the case in BasicEvictionOrdering.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-4);
    ASSERT_EQ(evicted_values_[i], i-4);
  }
}

// This tests the behavior of insert for a key that has a tombstone element in the
// cache. It should insert the new element as a protected element.
TEST_F(LIRSCacheTest, TombstoneToProtected) {
  FillCache();

  // Add one more element, which will evict element 95. It is now a tombstone.
  Insert(100, 100);
  ASSERT_EQ(evicted_values_[0], 95);
  ASSERT_EQ(evicted_keys_[0], 95);
  ASSERT_EQ(Lookup(95), -1);

  // If we insert 95 again, it will become a protected element due to the tombstone.
  Insert(95, 95);
  ASSERT_EQ(evicted_keys_[1], 96);
  ASSERT_EQ(evicted_values_[1], 96);
  evicted_values_.clear();
  evicted_keys_.clear();

  FlushCache();

  // The 5 unprotected elements are 97,98,99,100, 0 (pushed out of protected)

  // There were 5 unprotected elements (95-99). They are not impacted by changes in
  // the protected elements.
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(evicted_keys_[i], 97+i);
    ASSERT_EQ(evicted_values_[i], 97+i);
  }
  ASSERT_EQ(evicted_keys_[4], 0);
  ASSERT_EQ(evicted_values_[4], 0);

  // 0 was pushed out of protected, and 95 was added (at the tail of the recency queue).
  // So, this is just one offset from the case in BasicEvictionOrdering.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-4);
    ASSERT_EQ(evicted_values_[i], i-4);
  }
}

// This tests the case where there is a lookup of an unprotected element that has been
// trimmed from the recency queue. In this case, the unprotected element remains
// unprotected.
TEST_F(LIRSCacheTest, UnprotectedToUnprotected) {
  FillCache();

  // If we lookup every element that is protected, then the unprotected elements
  // will be trimmed from the recency queue. At that point, a lookup to the
  // unprotected elements will not change them to be protected.
  for (int i = 0; i < 95; ++i) {
    ASSERT_EQ(Lookup(i), i);
  }
  ASSERT_EQ(0, evicted_keys_.size());

  // The unprotected elements will remain in the unprotected list
  for (int i = 95; i < 100; ++i) {
    ASSERT_EQ(Lookup(i), i);
  }
  ASSERT_EQ(0, evicted_keys_.size());

  FlushCache();

  // All of this means that the results are the same as BasicEvictionOrdering
  // There were 5 unprotected elements (95-99). They should be the first to be evicted.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // There were 95 protected elements (0-94). They should be evicted in order.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// This tests the edge case where there is exactly one unprotected element
// and that element is looked up and remains unprotected.
TEST_F(LIRSCacheTest, ExactlyOneUnprotectedToUnprotected) {
  FillCache();

  // If we lookup every element that is protected, then the unprotected elements
  // will be trimmed from the recency queue. At that point, a lookup to the
  // unprotected elements will not change them to be protected.
  for (int i = 0; i < 95; ++i) {
    ASSERT_EQ(Lookup(i), i);
  }
  ASSERT_EQ(0, evicted_keys_.size());

  // There are 5 unprotected right now. Erase 4 of them (95-98) so that only one remains.
  for (int i = 95; i < 99; ++i) {
    Erase(i);
  }
  ASSERT_EQ(4, evicted_keys_.size());

  // Lookup the only remaining element (#99). This is unprotected without being in
  // the recency list, so it stays unprotected.
  ASSERT_EQ(Lookup(99), 99);
  ASSERT_EQ(4, evicted_keys_.size());

  FlushCache();

  // All of this means that the results are the same as BasicEvictionOrdering
  // There were 5 unprotected elements (95-99). They should be the first to be evicted.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // There were 95 protected elements (0-94). They should be evicted in order.
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// This tests that Erase works for both unprotected and protected elements.
TEST_F(LIRSCacheTest, Erase) {
  FillCache();

  // Erase a protected element
  Erase(25);
  ASSERT_EQ(evicted_keys_[0], 25);
  ASSERT_EQ(evicted_values_[0], 25);

  // Erase an unprotected element
  Erase(95);
  ASSERT_EQ(evicted_keys_[1], 95);
  ASSERT_EQ(evicted_values_[1], 95);

  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // There were 5 unprotected elements (95-99). Element 95 was erased. Verify the
  // remaining 4 are appropriate.
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(evicted_keys_[i], 96+i);
    ASSERT_EQ(evicted_values_[i], 96+i);
  }
  // There were 95 protected elements (0-94). Element 25 was erased. Verify the
  // two chunks.
  for (int i = 0; i < 25; ++i) {
    ASSERT_EQ(evicted_keys_[i+4], i);
    ASSERT_EQ(evicted_values_[i+4], i);
  }
  for (int i = 25; i < 94; ++i) {
    ASSERT_EQ(evicted_keys_[i+4], i + 1);
    ASSERT_EQ(evicted_values_[i+4], i + 1);
  }
}

// This is the same as InsertExistingProtectedNeedsTrim, except that it is verifying
// that erasing the last protected key on the recency list will trim the recency
// list.
TEST_F(LIRSCacheTest, EraseNeedsTrim) {
  FillCache();

  // Lookup every protected value except #25
  // This doesn't change which keys are protected vs unprotected, but
  // it changes the order of the elements on the recency list.
  // 25 will be the oldest element, then there will be all of the
  // unprotected elements, then all the other protected elements.
  for (int i = 0; i < 95; ++i) {
    if (i != 25) {
      ASSERT_EQ(Lookup(i), i);
    }
  }

  // Replace this last protected key with a new value. When this entry is inserted,
  // it will remove the old protected key. This makes the unprotected entries the
  // last on the list, so they will be trimmed off of the recency list.
  Erase(25);
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(evicted_keys_[0], 25);
  ASSERT_EQ(evicted_values_[0], 25);
  evicted_keys_.clear();
  evicted_values_.clear();

  // If the recency list wasn't trimmed appropriately, then this would hit an assert.
  FlushCache();

  // There were 5 unprotected elements (95-99). They were not impacted by changes in
  // the protected elements.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], 95+i);
    ASSERT_EQ(evicted_values_[i], 95+i);
  }
  // The only thing we guarantee is that key 25 is gone. None of the other
  // protected elements moved around, but the exact ordering is not specified.
  for (int i = 5; i < 99; ++i) {
    ASSERT_NE(evicted_keys_[i], 25);
    ASSERT_LT(evicted_keys_[i], 95);
    ASSERT_GE(evicted_keys_[i], 0);
    ASSERT_EQ(evicted_values_[i], evicted_keys_[i]);
  }
}

// This tests the enforcement of the tombstone limit. The lirs_tombstone_multiple is
// 2.0, so we expect a cache with 100 elements to maintain at most 200 tombstones.
TEST_F(LIRSCacheTest, TombstoneLimit1) {
  // Fill the cache to the point where there are 100 normal elements and 200 tombstones.
  FillCacheToTombstoneLimit();

  // Now, add one final element
  Insert(300, 300);
  // Item 295 was evicted and became a tombstone
  ASSERT_EQ(evicted_keys_[0], 295);
  ASSERT_EQ(evicted_values_[0], 295);
  ASSERT_EQ(evicted_keys_.size(), 1);
  evicted_keys_.clear();
  evicted_values_.clear();

  // Item 95 was a tombstone, but we expect it to be removed due to the tombstone limit.
  // When we insert it, it will be unprotected.
  Insert(95, 95);
  ASSERT_EQ(evicted_keys_.size(), 1);
  ASSERT_EQ(evicted_keys_[0], 296);
  ASSERT_EQ(evicted_values_[0], 296);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // The 5 unprotected elements are 297, 298, 299, 300, 95
  vector<int> unprotected_elems = {297, 298, 299, 300, 95};
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], unprotected_elems[i]);
    ASSERT_EQ(evicted_values_[i], unprotected_elems[i]);
  }
  // The protected elements are unaffected, so this is the same as BasicEvictionOrdering
  for (int i = 5; i < 100; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-5);
    ASSERT_EQ(evicted_values_[i], i-5);
  }
}

// This tests the enforcement of the tombstone limit. The lirs_tombstone_multiple is
// 2.0, so we expect a cache with 100 elements to maintain at most 200 tombstones.
TEST_F(LIRSCacheTest, TombstoneLimit2) {
  // Fill the cache to the point where there are 100 normal elements and 200 tombstones.
  FillCacheToTombstoneLimit();

  // Now, add one final element
  Insert(300, 300);
  // Item 295 was evicted and became a tombstone
  ASSERT_EQ(evicted_keys_[0], 295);
  ASSERT_EQ(evicted_values_[0], 295);
  ASSERT_EQ(evicted_keys_.size(), 1);
  evicted_keys_.clear();
  evicted_values_.clear();

  // Item 95 was a tombstone, and it should have been removed due to the tombstone limit.
  // Item 96 was not removed, so when we insert it, it will be protected.
  Insert(96, 96);
  ASSERT_EQ(evicted_keys_.size(), 1);
  ASSERT_EQ(evicted_keys_[0], 296);
  ASSERT_EQ(evicted_values_[0], 296);
  evicted_keys_.clear();
  evicted_values_.clear();

  FlushCache();

  // The 5 unprotected elements are 297, 298, 299, 300, 0 (pushed out of protected)
  vector<int> unprotected_elems = {297, 298, 299, 300, 0};
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(evicted_keys_[i], unprotected_elems[i]);
    ASSERT_EQ(evicted_values_[i], unprotected_elems[i]);
  }
  // 96 was added a protected element, so items 5-99 are 1-94.
  for (int i = 5; i < 99; ++i) {
    ASSERT_EQ(evicted_keys_[i], i-4);
    ASSERT_EQ(evicted_values_[i], i-4);
  }
  ASSERT_EQ(evicted_keys_[99], 96);
  ASSERT_EQ(evicted_values_[99], 96);
}

// This tests the enforcement of the tombstone limit. This is a simple test that
// verifies we can free multiple tombstones at once.
TEST_F(LIRSCacheTest, TombstoneLimitFreeMultiple) {
  // Fill the cache to the point where there are 100 normal elements and 200 tombstones.
  FillCacheToTombstoneLimit();

  // Inserting an unprotected element with a large size does two things. First, it
  // evicts all the current unprotected and makes them tombstones. Second, the number
  // of non-tombstones shrank, so this will free multiple tombstones.
  // This test is primarily focused on not crashing.
  ASSERT_TRUE(Insert(500, 500, 5));
  ASSERT_EQ(evicted_keys_.size(), 5);
  ASSERT_EQ(evicted_values_.size(), 5);
}

} // namespace impala
