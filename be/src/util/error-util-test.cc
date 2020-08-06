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

#include <gutil/strings/substitute.h>

#include "gen-cpp/Status_types.h"
#include "gen-cpp/ErrorCodes_types.h"

#include "error-util-internal.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

constexpr int ErrorMsg::MAX_ERROR_MESSAGE_LEN;

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

  // Test long error message and truncation.
  string long_msg = std::string(256 * 1024, '-'); // 256kb string
  msg = ErrorMsg(TErrorCode::GENERAL, long_msg);
  ASSERT_EQ(ErrorMsg::MAX_ERROR_MESSAGE_LEN, msg.msg().size());
}

TEST(ErrorMsg, MergeMap) {
  ErrorLogMap left, right;
  left[TErrorCode::GENERAL].add_messages("1");

  right[TErrorCode::GENERAL].add_messages("2");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].add_messages("p");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].set_count(3);

  MergeErrorMaps(right, &left);
  ASSERT_EQ(2, left.size());
  ASSERT_EQ(2, left[TErrorCode::GENERAL].messages_size());

  right = ErrorLogMap();
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].add_messages("p");
  right[TErrorCode::PARQUET_MULTIPLE_BLOCKS].set_count(3);

  MergeErrorMaps(right, &left);
  ASSERT_EQ(2, left.size());
  ASSERT_EQ(2, left[TErrorCode::GENERAL].messages_size());
  ASSERT_EQ(6, left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count());

  ErrorLogMap dummy, cleared;
  dummy[TErrorCode::GENERAL].add_messages("2");
  dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].add_messages("p");
  dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].set_count(3);
  ASSERT_EQ(2, dummy.size());
  ASSERT_EQ(3, dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count());
  ASSERT_EQ(1, dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages_size());
  ASSERT_EQ(1, dummy[TErrorCode::GENERAL].messages_size());
  cleared[TErrorCode::GENERAL].add_messages("1");
  cleared[TErrorCode::RPC_RECV_TIMEOUT].add_messages("p");
  ClearErrorMap(cleared);
  ASSERT_EQ(2, cleared.size());
  ASSERT_EQ(1, cleared.count(TErrorCode::RPC_RECV_TIMEOUT));

  MergeErrorMaps(cleared, &dummy);
  ASSERT_EQ(3, dummy.size());
  ASSERT_EQ(3, dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count());
  ASSERT_EQ(1, dummy[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages_size());
  ASSERT_EQ(1, dummy.count(TErrorCode::RPC_RECV_TIMEOUT));
  ASSERT_EQ(0, dummy[TErrorCode::RPC_RECV_TIMEOUT].count());
  ASSERT_EQ(0, dummy[TErrorCode::RPC_RECV_TIMEOUT].messages_size());
  ASSERT_EQ(0, dummy[TErrorCode::GENERAL].count());
  ASSERT_EQ(1, dummy[TErrorCode::GENERAL].messages_size());

  MergeErrorMaps(dummy, &cleared);
  ASSERT_EQ(3, cleared.size());
  ASSERT_EQ(3, cleared[TErrorCode::PARQUET_MULTIPLE_BLOCKS].count());
  ASSERT_EQ(1, cleared[TErrorCode::PARQUET_MULTIPLE_BLOCKS].messages_size());
  ASSERT_EQ(1, cleared.count(TErrorCode::RPC_RECV_TIMEOUT));
  ASSERT_EQ(0, cleared[TErrorCode::RPC_RECV_TIMEOUT].count());
  ASSERT_EQ(0, cleared[TErrorCode::RPC_RECV_TIMEOUT].messages_size());
  ASSERT_EQ(0, cleared[TErrorCode::GENERAL].count());
  ASSERT_EQ(1, cleared[TErrorCode::GENERAL].messages_size());
}

TEST(ErrorMsg, CountErrors) {
  ErrorLogMap m;
  ASSERT_EQ(0, ErrorCount(m));
  m[TErrorCode::PARQUET_MULTIPLE_BLOCKS].add_messages("p");
  m[TErrorCode::PARQUET_MULTIPLE_BLOCKS].set_count(999);
  ASSERT_EQ(1, ErrorCount(m));
  m[TErrorCode::GENERAL].add_messages("1");
  m[TErrorCode::GENERAL].add_messages("2");
  ASSERT_EQ(3, ErrorCount(m));
  ClearErrorMap(m);
  ASSERT_EQ(1, ErrorCount(m));
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
  ClearErrorMap(m);
  ASSERT_EQ(1, ErrorCount(m));
}

TEST(ErrorMsg, PrintMap) {
  ErrorLogMap left;
  left[TErrorCode::GENERAL].add_messages("1");
  left[TErrorCode::GENERAL].add_messages("2");
  left[TErrorCode::GENERAL].add_messages("3");
  left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].add_messages("p");
  left[TErrorCode::PARQUET_MULTIPLE_BLOCKS].set_count(999);
  const string msg = PrintErrorMapToString(left);
  ASSERT_EQ("1\n2\n3\np (1 of 999 similar)\n", msg);
  ClearErrorMap(left);
  ASSERT_EQ("", PrintErrorMapToString(left));
}

}

