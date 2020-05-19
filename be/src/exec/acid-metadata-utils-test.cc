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

#include <gtest/gtest.h>

#include "testutil/gtest-util.h"
#include "exec/acid-metadata-utils.h"

#include "gen-cpp/CatalogObjects_types.h"

#include "common/names.h"

using namespace impala;

namespace {

TValidWriteIdList MakeTValidWriteIdList(int64_t high_watermak,
    const std::vector<int64_t>& invalid_write_ids = {},
    const std::vector<int>& aborted_indexes = {}) {
  TValidWriteIdList ret;
  ret.__set_high_watermark(high_watermak);
  ret.__set_invalid_write_ids(invalid_write_ids);
  ret.__set_aborted_indexes(aborted_indexes);
  return ret;
}

} // anonymous namespace

TEST(ValidWriteIdListTest, IsWriteIdValid) {
  ValidWriteIdList write_id_list;
  // Everything is valid by default.
  EXPECT_TRUE(write_id_list.IsWriteIdValid(1));
  EXPECT_TRUE(write_id_list.IsWriteIdValid(1000));
  EXPECT_TRUE(write_id_list.IsWriteIdValid(std::numeric_limits<int64_t>::max()));

  // Write ids <= 10 are valid
  write_id_list.InitFrom(MakeTValidWriteIdList(10));
  for (int i = 1; i <= 10; ++i) {
    EXPECT_TRUE(write_id_list.IsWriteIdValid(i));
  }
  EXPECT_FALSE(write_id_list.IsWriteIdValid(11));

  // Test open write ids
  write_id_list.InitFrom(MakeTValidWriteIdList(10, {1, 3, 5, 7, 9}));
  for (int i = 1; i <= 10; ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(write_id_list.IsWriteIdValid(i));
    } else {
      EXPECT_FALSE(write_id_list.IsWriteIdValid(i));
    }
  }
  EXPECT_FALSE(write_id_list.IsWriteIdValid(11));
  EXPECT_FALSE(write_id_list.IsWriteIdValid(12));

  // Test aborted write ids
  write_id_list.InitFrom(MakeTValidWriteIdList(10, {1, 3, 5, 7, 9}, {0, 1, 2, 3, 4}));
  for (int i = 1; i <= 10; ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(write_id_list.IsWriteIdValid(i));
    } else {
      EXPECT_FALSE(write_id_list.IsWriteIdValid(i));
    }
  }
  EXPECT_FALSE(write_id_list.IsWriteIdValid(11));
  EXPECT_FALSE(write_id_list.IsWriteIdValid(12));

    // Test open and aborted write ids
  write_id_list.InitFrom(MakeTValidWriteIdList(10, {1, 3, 5, 7, 9}, {3, 4}));
  for (int i = 1; i <= 10; ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(write_id_list.IsWriteIdValid(i));
    } else {
      EXPECT_FALSE(write_id_list.IsWriteIdValid(i));
    }
  }
  EXPECT_FALSE(write_id_list.IsWriteIdValid(11));
  EXPECT_FALSE(write_id_list.IsWriteIdValid(12));
}

TEST(ValidWriteIdListTest, IsWriteIdRangeValid) {
  ValidWriteIdList write_id_list;
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(1, 2));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(2, 200));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(100, 100000));

  write_id_list.InitFrom(
      MakeTValidWriteIdList(110, {1,2,3,5,7,9,11,31,50,55,90,97,98,99}));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsWriteIdRangeValid(1, 2));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(2, 200));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(100, 100000));

  write_id_list.InitFrom(MakeTValidWriteIdList(1000000,
      {1,2,3,5,7,9,11,31,50,55,90,97,98,99,110,1000,1100,2000,10000,150000,550000,90000,
      100000,110000,111000,222000,333000,444000,555000,666000,777000,888000,999000,999999}
  ));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsWriteIdRangeValid(1, 2));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(2, 200));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(100, 100000));

  write_id_list.InitFrom(
      MakeTValidWriteIdList(1000000, {555000,666000,777000,888000,999000,999999}));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(1, 2));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(2, 200));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(100, 100000));

  write_id_list.InitFrom(MakeTValidWriteIdList(1000, {500,600,700,800,900}));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsWriteIdRangeValid(1100, 2000));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(900, 1100));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(901, 1000));

  write_id_list.InitFrom(MakeTValidWriteIdList(1000));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsWriteIdRangeValid(1100, 1200));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsWriteIdRangeValid(900, 1100));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsWriteIdRangeValid(90, 950));
}

TEST(ValidWriteIdListTest, IsFileRangeValid) {
  ValidWriteIdList write_id_list;
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_1_2/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_1_2/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_5_5/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_5_5/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delta_100_1100/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_100_1100/0000"));

  write_id_list.InitFrom(
      MakeTValidWriteIdList(110, {1,2,3,5,7,9,11,31,50,55,90,97,98,99}));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_1_2/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_1_2/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_2_2/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_1_1/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_2_200/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_2_200/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_100_100000/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_100_100000/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_100_100/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_100_100/0000"));

  write_id_list.InitFrom(MakeTValidWriteIdList(1000, {500,600,700,800,900}));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_1100_2000/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_1100_2000/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_800_800/0000"));
  EXPECT_EQ(ValidWriteIdList::NONE, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_500_500/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_900_1100/0000"));
  EXPECT_EQ(ValidWriteIdList::SOME, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_900_1100/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_901_1000/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_901_1000/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
      "/foo/bar/delta_901_901/0000"));
  EXPECT_EQ(ValidWriteIdList::ALL, write_id_list.IsFileRangeValid(
    "/foo/bar/delete_delta_901_901/0000"));
}

TEST(ValidWriteIdListTest, IsCompacted) {
  EXPECT_TRUE(ValidWriteIdList::IsCompacted("/foo/delta_00005_00010_v123/000"));
  EXPECT_TRUE(ValidWriteIdList::IsCompacted("/foo/delete_delta_00001_00010_v123/000"));
  EXPECT_TRUE(ValidWriteIdList::IsCompacted("/foo/base_00001_v0123/000"));

  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delta_1_1/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delta_1_2/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delta_5_10/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delta_05_09/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delta_0000010_0000030/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delete_delta_1_2/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delete_delta_5_10/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delete_delta_05_09/0000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/delete_delta_000010_000030/000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/base_10/000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/000"));
  EXPECT_FALSE(ValidWriteIdList::IsCompacted("/foo/p=1/000"));
}

TEST(ValidWriteIdListTest, GetWriteIdRange) {
  EXPECT_EQ((make_pair<int64_t, int64_t>(0, 0)),
      ValidWriteIdList::GetWriteIdRange("/foo/00000_0"));
  EXPECT_EQ((make_pair<int64_t, int64_t>(5, 5)),
      ValidWriteIdList::GetWriteIdRange("/foo/base_00005/000"));
  EXPECT_EQ((make_pair<int64_t, int64_t>(5, 5)),
      ValidWriteIdList::GetWriteIdRange("/foo/base_00005_v123/000"));
  EXPECT_EQ((make_pair<int64_t, int64_t>(5 ,10)),
      ValidWriteIdList::GetWriteIdRange("/foo/delta_00005_00010/000"));
  EXPECT_EQ((make_pair<int64_t, int64_t>(5 ,10)),
      ValidWriteIdList::GetWriteIdRange("/foo/delta_00005_00010_0006/000"));
  EXPECT_EQ((make_pair<int64_t, int64_t>(5 ,10)),
      ValidWriteIdList::GetWriteIdRange("/foo/delta_00005_00010_v123/000"));
}

TEST(ValidWriteIdListTest, GetBucketProperty) {
  EXPECT_EQ(536870912, ValidWriteIdList::GetBucketProperty("/foo/0000000_0"));
  EXPECT_EQ(536936448, ValidWriteIdList::GetBucketProperty("/foo/0000001_1"));
  EXPECT_EQ(537001984, ValidWriteIdList::GetBucketProperty("/foo/bucket_00002"));
  EXPECT_EQ(537067520, ValidWriteIdList::GetBucketProperty(
      "/foo/base_0001_v1/bucket_000003_0"));
  EXPECT_EQ(537133056, ValidWriteIdList::GetBucketProperty(
      "/foo/delta_1_5/bucket_0000004_1"));
  EXPECT_EQ(537198592, ValidWriteIdList::GetBucketProperty(
      "/foo/delta_1_1_v1/000005_0_copy_1"));
  EXPECT_EQ(536870913, ValidWriteIdList::GetBucketProperty(
      "/foo/delta_1_1_1/00000_0"));
}
