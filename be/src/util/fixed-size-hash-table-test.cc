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

#include "testutil/gtest-util.h"
#include "util/fixed-size-hash-table.h"

#include "common/names.h"

namespace impala {

/// Basic test for all operations.
TEST(FixedSizeHash, TestAllOperations) {
  for (int seed = 0; seed < 100; ++seed) {
    // Allocate large enough to just fit keys.
    // Try multiple hash seeds so that we will get collisions.
    FixedSizeHashTable<void*, int> tbl;
    EXPECT_TRUE(tbl.Init(4, seed).ok());
    void* k1 = (void*)1;
    void* k2 = (void*)2;
    void* k3 = (void*)3;
    int v1 = 4;
    int v2 = 5;
    int v3 = 6;
    int result;
    const int* result_ptr;

    // Table should be initially empty.
    EXPECT_FALSE(tbl.Find(k1, &result));
    EXPECT_FALSE(tbl.Find(k2, &result));
    EXPECT_FALSE(tbl.Find(k3, &result));
    EXPECT_EQ(0, tbl.size());
    tbl.Insert(k1, v1);
    tbl.Insert(k2, v2);
    tbl.Insert(k3, v3);
    EXPECT_EQ(3, tbl.size());
    EXPECT_TRUE(tbl.Find(k1, &result));
    EXPECT_EQ(v1, result);
    EXPECT_TRUE(tbl.Find(k2, &result));
    EXPECT_EQ(v2, result);
    EXPECT_TRUE(tbl.Find(k3, &result));
    EXPECT_EQ(v3, result);
    // Exercise conditional insertion functions.
    EXPECT_FALSE(tbl.InsertIfNotPresent(k1, v2));
    EXPECT_EQ(v1, *tbl.FindOrInsert(k1, v2));
    EXPECT_EQ(v3, result);
    EXPECT_EQ(3, tbl.size());
    // Check that table is cleared correctly.
    tbl.Clear();
    EXPECT_EQ(0, tbl.size());
    EXPECT_FALSE(tbl.Find(k1, &result));
    EXPECT_FALSE(tbl.Find(k2, &result));
    EXPECT_FALSE(tbl.Find(k3, &result));
    // Exercise conditional insertion more.
    EXPECT_TRUE(tbl.InsertIfNotPresent(k1, v2));
    EXPECT_TRUE(tbl.Find(k1, &result));
    EXPECT_EQ(v2, result);
    EXPECT_EQ(1, tbl.size());
    result_ptr = tbl.FindOrInsert(k2, v3);
    EXPECT_EQ(v3, *result_ptr);
    EXPECT_TRUE(tbl.Find(k2, &result));
    EXPECT_EQ(v3, result);
    EXPECT_EQ(2, tbl.size());
  }
}

}

