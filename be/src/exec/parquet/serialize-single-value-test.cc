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

#include <string>

#include "parquet-column-stats.inline.h"
#include "runtime/decimal-value.inline.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

TEST(SerializeSingleValueTest, Decimal) {
  Decimal4Value d4;
  Decimal8Value d8;
  bool overflow = false;
  std::string out;

  d4 = Decimal4Value::FromInt(9, 4, -1, &overflow);
  EXPECT_FALSE(overflow);
  ColumnStats<Decimal4Value>::SerializeIcebergSingleValue(d4, &out);
  // -10000 is 0xffffd8f0, but the result contains only the minimum number of bytes.
  // Only 2 bytes are required.
  EXPECT_EQ(std::string("\xD8\xF0", 2), out);

  d4 = Decimal4Value::FromInt(9, 0, -1, &overflow);
  EXPECT_FALSE(overflow);
  ColumnStats<Decimal4Value>::SerializeIcebergSingleValue(d4, &out);
  // -1 is 0xffffffff, but the result contains only the minimum number of bytes.
  // Only 1 byte is required.
  EXPECT_EQ(std::string("\xFF", 1), out);

  d8 = Decimal8Value::FromInt(18, 0, -2147483647, &overflow);
  EXPECT_FALSE(overflow);
  ColumnStats<Decimal8Value>::SerializeIcebergSingleValue(d8, &out);
  // -2147483647 is 0xffffffff80000001. Only 4 bytes are required.
  EXPECT_EQ(std::string("\x80\x00\x00\x01", 4), out);

  d4 = Decimal4Value::FromInt(9, 4, 0, &overflow);
  EXPECT_FALSE(overflow);
  ColumnStats<Decimal4Value>::SerializeIcebergSingleValue(d4, &out);
  // 0 is 0x00, 1 byte is required
  EXPECT_EQ(std::string("\x00", 1), out);

  d8 = Decimal8Value::FromInt(18, 0, 2147483647, &overflow);
  EXPECT_FALSE(overflow);
  ColumnStats<Decimal8Value>::SerializeIcebergSingleValue(d8, &out);
  // 2147483647 is 0x000000007fffffff. Only 4 bytes are required.
  EXPECT_EQ(std::string("\x7F\xFF\xFF\xFF", 4), out);
}
}
