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
#include "util/in-list-filter.h"

#include "runtime/date-value.h"
#include "runtime/test-env.h"

using namespace impala;

template<typename T, PrimitiveType SLOT_TYPE>
void TestNumericInListFilter() {
  MemTracker mem_tracker;
  ObjectPool obj_pool;
  ColumnType col_type(SLOT_TYPE);
  InListFilter* filter = InListFilter::Create(col_type, 20, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  for (T v = -10; v < 10; ++v) {
    filter->Insert(&v);
  }
  // Insert duplicated values again
  for (T v = 9; v >= 0; --v) {
    filter->Insert(&v);
  }
  EXPECT_EQ(20, filter->NumItems());
  EXPECT_FALSE(filter->ContainsNull());
  filter->Insert(nullptr);
  EXPECT_TRUE(filter->ContainsNull());
  EXPECT_EQ(21, filter->NumItems());

  for (T v = -10; v < 10; ++v) {
    EXPECT_TRUE(filter->Find(&v, col_type));
  }
  T i = -11;
  EXPECT_FALSE(filter->Find(&i, col_type));
  i = 10;
  EXPECT_FALSE(filter->Find(&i, col_type));

  EXPECT_FALSE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  // Test falling back to an always_true filter when #items exceeds the limit
  filter->Insert(&i);
  EXPECT_FALSE(filter->AlwaysFalse());
  EXPECT_TRUE(filter->AlwaysTrue());
  EXPECT_EQ(0, filter->NumItems());
}

TEST(InListFilterTest, TestTinyint) {
  TestNumericInListFilter<int8_t, TYPE_TINYINT>();
}

TEST(InListFilterTest, TestSmallint) {
  TestNumericInListFilter<int16_t, TYPE_SMALLINT>();
}

TEST(InListFilterTest, TestInt) {
  TestNumericInListFilter<int32_t, TYPE_INT>();
}

TEST(InListFilterTest, TestBigint) {
  TestNumericInListFilter<int64_t, TYPE_BIGINT>();
}

TEST(InListFilterTest, TestDate) {
  MemTracker mem_tracker;
  ObjectPool obj_pool;
  ColumnType col_type(TYPE_DATE);
  InListFilter* filter = InListFilter::Create(col_type, 5, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  vector<DateValue> values;
  for (int i = 1; i <= 5; ++i) {
    values.emplace_back(i * 10000);
  }

  for (const auto& v : values) {
    filter->Insert(&v);
  }
  // Insert duplicated values again
  for (const auto& v : values) {
    filter->Insert(&v);
  }
  EXPECT_EQ(5, filter->NumItems());
  EXPECT_FALSE(filter->ContainsNull());
  filter->Insert(nullptr);
  EXPECT_TRUE(filter->ContainsNull());
  EXPECT_EQ(6, filter->NumItems());

  for (const auto& v : values) {
    EXPECT_TRUE(filter->Find(&v, col_type));
  }
  DateValue d(60000);
  EXPECT_FALSE(filter->Find(&d, col_type));
}

void TestStringInListFilter(const ColumnType& col_type) {
  MemTracker mem_tracker;
  ObjectPool obj_pool;
  InListFilter* filter = InListFilter::Create(col_type, 5, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  vector<StringValue> ss1 = { StringValue("aaa"), StringValue("aa") };
  vector<StringValue> ss2 = { StringValue("a"), StringValue("b"), StringValue("c") };

  // Insert the first batch
  for (const StringValue& s : ss1) {
    filter->Insert(&s);
  }
  filter->MaterializeValues();
  // Insert the second batch with some duplicated values
  for (const StringValue& s : ss2) {
    filter->Insert(&s);
  }
  for (const StringValue& s : ss1) {
    filter->Insert(&s);
  }
  filter->MaterializeValues();

  EXPECT_EQ(5, filter->NumItems());
  EXPECT_FALSE(filter->ContainsNull());
  filter->Insert(nullptr);
  EXPECT_TRUE(filter->ContainsNull());
  EXPECT_EQ(6, filter->NumItems());

  // Merge ss2 to ss1
  ss1.insert(ss1.end(), ss2.begin(), ss2.end());
  for (const StringValue& s : ss1) {
    EXPECT_TRUE(filter->Find(&s, col_type));
  }
  StringValue d("d");
  EXPECT_FALSE(filter->Find(&d, col_type));
  filter->Close();
}

TEST(InListFilterTest, TestString) {
  ColumnType col_type(TYPE_STRING);
  TestStringInListFilter(col_type);
}

TEST(InListFilterTest, TestVarchar) {
  ColumnType col_type = ColumnType::CreateVarcharType(10);
  TestStringInListFilter(col_type);
}

TEST(InListFilterTest, TestChar) {
  MemTracker mem_tracker;
  ObjectPool obj_pool;
  ColumnType col_type = ColumnType::CreateCharType(2);
  InListFilter* filter = InListFilter::Create(col_type, 5, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  char str_buffer[] = "aabbccddeeff";
  const char* ptr = str_buffer;
  // Insert 3 values first
  for (int i = 0; i < 3; ++i) {
    filter->Insert(ptr);
    ptr += 2;
  }
  filter->MaterializeValues();
  // Insert the 5 all values
  ptr = str_buffer;
  for (int i = 0; i < 5; ++i) {
    filter->Insert(ptr);
    ptr += 2;
  }
  filter->MaterializeValues();

  EXPECT_EQ(5, filter->NumItems());
  EXPECT_FALSE(filter->ContainsNull());
  filter->Insert(nullptr);
  EXPECT_TRUE(filter->ContainsNull());
  EXPECT_EQ(6, filter->NumItems());

  ptr = str_buffer;
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(filter->Find(ptr, col_type));
    ptr += 2;
  }
  ptr = "gg";
  EXPECT_FALSE(filter->Find(ptr, col_type));
  filter->Close();
}