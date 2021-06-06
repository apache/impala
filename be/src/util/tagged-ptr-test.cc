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

#include "util/tagged-ptr.h"
#include <string>
#include <boost/preprocessor/repetition/repeat_from_to.hpp>
#include "testutil/gtest-util.h"

namespace impala {

class TaggedPtrTest {
 public:
  int id;
  TaggedPtrTest(int i, const std::string& s) {
    id = i;
    str = s;
  }

  std::string GetString() { return str; }

  bool operator==(const TaggedPtrTest& other) {
    return id == other.id && str == other.str;
  }

 private:
  std::string str;
};

TaggedPtr<TaggedPtrTest> MakeTaggedPtr(int a, const std::string& s) {
  return TaggedPtr<TaggedPtrTest>::make_tagptr(a, s);
}

union TestData {
  int x;
  float y;
  const char* s;
};

struct TestBucket; // Forward Declaration.

union TestBucketData {
  TestData data;
  TestBucket* next;
};

class TaggedBucketData : public TaggedPtr<TestBucketData, false> {
 public:
  bool IsData() { return IsTagBitSet<0>(); }
  void SetIsData() { SetTagBit<0>(); }
  void UnsetIsData() { UnsetTagBit<0>(); }
  TestBucketData* GetData() { return GetPtr(); }
  void SetBucketData(uintptr_t address) { SetPtr((TestBucketData*)address); }
};

struct TestBucket {
  int id;
  TaggedBucketData bucketData;
};

TEST(TaggedPtrTest, Simple) {
  auto ptr = MakeTaggedPtr(3, "test1");
  // Test for non-null
  EXPECT_FALSE(!ptr);

  // Test dereference operator
  EXPECT_EQ((*ptr).id, 3);
  EXPECT_EQ(ptr->GetString(), std::string("test1"));

  // Test initial tag
  EXPECT_EQ(ptr.GetTag(), 0);

  // Set/Unset all tag bits and check
#pragma push_macro("TEST_ALL_TAG_BITS")
#define TEST_ALL_TAG_BITS(ignore1, i, ignore2) \
  ptr.SetTagBit<i>();                          \
  EXPECT_TRUE(ptr.IsTagBitSet<i>());           \
  ptr.UnsetTagBit<i>();                        \
  EXPECT_FALSE(ptr.IsTagBitSet<i>());

  BOOST_PP_REPEAT_FROM_TO(0, 6, TEST_ALL_TAG_BITS, ignore);
#pragma pop_macro("TEST_ALL_TAG_BITS")

  // Set few tag bits and check
  ptr.SetTagBit<0>();
  ptr.SetTagBit<1>();
  EXPECT_TRUE(ptr.IsTagBitSet<0>());
  EXPECT_TRUE(ptr.IsTagBitSet<1>());
  EXPECT_FALSE(ptr.IsTagBitSet<2>());
  EXPECT_EQ(ptr.GetTag(), 0X60);

  // Unset Tag
  ptr.UnsetTagBit<0>();
  EXPECT_FALSE(ptr.IsTagBitSet<0>());
  EXPECT_EQ(ptr.GetTag(), 0x20);

  // Move Semantics
  auto ptr_move1 = std::move(ptr);
  EXPECT_FALSE(!ptr_move1);
  EXPECT_TRUE(!ptr);
  TaggedPtr<TaggedPtrTest> ptr_move2;
  EXPECT_TRUE(!ptr_move2);
  ptr_move2 = std::move(ptr_move1);
  EXPECT_FALSE(!ptr_move2);
}

TEST(TaggedPtrTest, Comparision) {
  auto ptr1 = MakeTaggedPtr(3, "test1");
  auto ptr2 = MakeTaggedPtr(3, "test2");
  auto ptr3 = MakeTaggedPtr(3, "test1");
  ptr1.SetTagBit<1>();
  ptr1.SetTagBit<2>();
  ptr3.SetTagBit<1>();
  ptr3.SetTagBit<2>();
  EXPECT_FALSE(ptr1 == ptr3);
  EXPECT_TRUE(ptr1 != ptr2);
  EXPECT_TRUE(*ptr1 == *ptr3);
  EXPECT_FALSE(*ptr1 == *ptr2);
}
TEST(TaggedPtrTest, Complex) {
  // Check if tag bits are retained on setting Data
  TestBucket tagTest;
  tagTest.bucketData.SetIsData();
  tagTest.bucketData.SetTagBit<1>();
  TestBucketData tag_bucket_data;
  tag_bucket_data.data.s = "TagTest";
  tagTest.bucketData.SetBucketData((uintptr_t)&tag_bucket_data);
  EXPECT_TRUE(tagTest.bucketData.IsData());
  EXPECT_TRUE(tagTest.bucketData.IsTagBitSet<1>());
  EXPECT_EQ(tagTest.bucketData.GetData()->data.s, "TagTest");
  // set to null and check
  tagTest.bucketData.SetBucketData(0);
  EXPECT_TRUE(tagTest.bucketData.IsNull());
  EXPECT_TRUE(tagTest.bucketData.IsData());
  EXPECT_TRUE(tagTest.bucketData.IsTagBitSet<1>());

  // Creating two buckets bucket1 and bucket2 and linking bucket2 to bucket1.
  TestBucket bucket1;
  bucket1.id = 1;
  TestBucketData bucket_data;
  bucket_data.data.s = "testString";
  TestBucket bucket2;
  bucket2.id = 2;
  bucket2.bucketData.SetBucketData((uintptr_t)&bucket_data);
  bucket2.bucketData.SetIsData();
  TestBucketData bucket_data1;
  bucket_data1.next = &bucket2;
  bucket1.bucketData.SetBucketData((uintptr_t)&bucket_data1);

  EXPECT_FALSE(bucket1.bucketData.IsData());
  auto first_bd = bucket1.bucketData.GetData();
  EXPECT_EQ(first_bd->next->id, 2);
  auto second_bd = first_bd->next->bucketData.GetData();
  EXPECT_EQ(second_bd->data.s, "testString");
}
}; // namespace impala
