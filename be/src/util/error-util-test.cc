// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <gutil/strings/substitute.h>

#include "error-util.h"
#include "gen-cpp/Status_types.h"
#include "gen-cpp/ErrorCodes_types.h"

namespace impala {

TEST(ErrorMsg, GenericFormatting) {
  ErrorMsg msg(TErrorCode::GENERAL, "This is a test");
  ASSERT_EQ("This is a test", msg.msg());

  msg.AddDetail("Detail come here.");
  msg.AddDetail("Or here.");
  ASSERT_EQ("This is a test\nDetail come here.\nOr here.\n",
      msg.GetFullMessageDetails());

  msg = ErrorMsg(TErrorCode::MISSING_BUILTIN, "fun", "sym");
  ASSERT_EQ("Builtin 'fun' with symbol 'sym' does not exist. Verify that "
      "all your impalads are the same version.", msg.msg());
}

TEST(ErrorMsg, MergeMap) {
  ErrorLogMap left, right;
  left[TErrorCode::GENERAL].messages.push_back("1");

  right[TErrorCode::GENERAL].messages.push_back("2");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages.push_back("p");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count = 3;

  MergeErrorMaps(&left, right);
  ASSERT_EQ(2, left.size());
  ASSERT_EQ(2, left[TErrorCode::GENERAL].messages.size());

  right = ErrorLogMap();
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages.push_back("p");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count = 3;

  MergeErrorMaps(&left, right);
  ASSERT_EQ(2, left.size());
  ASSERT_EQ(2, left[TErrorCode::GENERAL].messages.size());
  ASSERT_EQ(6, left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count);
}

TEST(ErrorMsg, CountErrors) {
  ErrorLogMap m;
  ASSERT_EQ(0, ErrorCount(m));
  m[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages.push_back("p");
  m[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count = 999;
  ASSERT_EQ(1, ErrorCount(m));
  m[TErrorCode::GENERAL].messages.push_back("1");
  m[TErrorCode::GENERAL].messages.push_back("2");
  ASSERT_EQ(3, ErrorCount(m));
}

TEST(ErrorMsg, AppendError) {
  ErrorLogMap m;
  ASSERT_EQ(0, ErrorCount(m));
  AppendError(&m, ErrorMsg(TErrorCode::GENERAL, "1"));
  AppendError(&m, ErrorMsg(TErrorCode::GENERAL, "2"));
  ASSERT_EQ(2, ErrorCount(m));
  AppendError(&m, ErrorMsg(TErrorCode::PARQUET_MULTIPLE_BLOCKS, "p1"));
  ASSERT_EQ(3, ErrorCount(m));
  AppendError(&m, ErrorMsg(TErrorCode::PARQUET_MULTIPLE_BLOCKS, "p2"));
  ASSERT_EQ(3, ErrorCount(m));
}

TEST(ErrorMsg, PrintMap) {
  ErrorLogMap left;
  left[TErrorCode::GENERAL].messages.push_back("1");
  left[TErrorCode::GENERAL].messages.push_back("2");
  left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages.push_back("p");
  left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count = 999;
  ASSERT_EQ("1\n2\np (1 of 999 similar)\n", PrintErrorMapToString(left));
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
