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

template<typename T>
void VerifyItems(InListFilter* f, ColumnType col_type, T min_value, T max_value,
    bool contains_null) {
  int num_items = max_value - min_value + 1;
  if (contains_null) ++num_items;
  EXPECT_EQ(num_items, f->NumItems());
  EXPECT_EQ(contains_null, f->ContainsNull());
  for (T v = min_value; v <= max_value; ++v) {
    EXPECT_TRUE(f->Find(&v, col_type)) << v << " not found in " << f->DebugString();
  }
  T i = min_value - 1;
  EXPECT_FALSE(f->Find(&i, col_type));
  i = max_value + 1;
  EXPECT_FALSE(f->Find(&i, col_type));

  EXPECT_FALSE(f->AlwaysFalse());
  EXPECT_FALSE(f->AlwaysTrue());
}

InListFilter* CloneFromProtobuf(InListFilter* filter, ColumnType col_type,
    uint32_t entry_limit, ObjectPool* pool, MemTracker* mem_tracker) {
  InListFilterPB pb;
  InListFilter::ToProtobuf(filter, &pb);
  return InListFilter::Create(pb, col_type, entry_limit, pool, mem_tracker);
}

template<typename T, PrimitiveType SLOT_TYPE>
void TestNumericInListFilter() {
  MemTracker mem_tracker;
  ObjectPool obj_pool;
  ColumnType col_type(SLOT_TYPE);
  InListFilter* filter = InListFilter::Create(col_type, 20, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  // Insert 20 values
  for (T v = -10; v < 10; ++v) {
    filter->Insert(&v);
  }
  // Insert some duplicated values again
  for (T v = 9; v >= 0; --v) {
    filter->Insert(&v);
  }
  VerifyItems<T>(filter, col_type, -10, 9, false);

  // Copy the filter through protobuf for testing InsertBatch()
  InListFilter* filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool,
      &mem_tracker);
  VerifyItems<T>(filter2, col_type, -10, 9, false);

  // Insert NULL
  filter->Insert(nullptr);
  VerifyItems<T>(filter, col_type, -10, 9, true);

  filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool, &mem_tracker);
  VerifyItems<T>(filter2, col_type, -10, 9, true);

  // Test falling back to an always_true filter when #items exceeds the limit
  T value = 10;
  filter->Insert(&value);
  filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool, &mem_tracker);

  int i = 0;
  for (InListFilter* f : {filter, filter2}) {
    EXPECT_FALSE(f->AlwaysFalse());
    EXPECT_TRUE(f->AlwaysTrue());
    EXPECT_EQ(0, f->NumItems()) << "Error in filter " << i;
    ++i;
  }
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
  InListFilter* filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool,
      &mem_tracker);
  EXPECT_EQ(5, filter2->NumItems());
  EXPECT_FALSE(filter2->ContainsNull());

  filter->Insert(nullptr);
  filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool, &mem_tracker);

  for (InListFilter* f : {filter, filter2}) {
    EXPECT_TRUE(f->ContainsNull());
    EXPECT_EQ(6, f->NumItems());
    for (const auto& v : values) {
      EXPECT_TRUE(f->Find(&v, col_type));
    }
    DateValue d(60000);
    EXPECT_FALSE(f->Find(&d, col_type));
  }
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

  InListFilter* filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool,
      &mem_tracker);
  EXPECT_EQ(5, filter2->NumItems());
  EXPECT_FALSE(filter2->ContainsNull());
  filter2->Close();

  filter->Insert(nullptr);
  filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool, &mem_tracker);

  // Merge ss2 to ss1
  ss1.insert(ss1.end(), ss2.begin(), ss2.end());
  for (InListFilter* f : {filter, filter2}) {
    EXPECT_TRUE(f->ContainsNull());
    EXPECT_EQ(6, f->NumItems());
    for (const StringValue& s : ss1) {
      EXPECT_TRUE(f->Find(&s, col_type));
    }
    StringValue d("d");
    EXPECT_FALSE(f->Find(&d, col_type));
    f->Close();
  }
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
  InListFilter* filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool,
      &mem_tracker);
  EXPECT_EQ(5, filter2->NumItems());
  EXPECT_FALSE(filter2->ContainsNull());
  filter2->Close();

  filter->Insert(nullptr);
  filter2 = CloneFromProtobuf(filter, col_type, 20, &obj_pool, &mem_tracker);
  for (InListFilter* f : {filter, filter2}) {
    EXPECT_TRUE(f->ContainsNull());
    EXPECT_EQ(6, f->NumItems());

    ptr = str_buffer;
    for (int i = 0; i < 5; ++i) {
      EXPECT_TRUE(f->Find(ptr, col_type));
      ptr += 2;
    }
    ptr = "gg";
    EXPECT_FALSE(f->Find(ptr, col_type));
    f->Close();
  }
}