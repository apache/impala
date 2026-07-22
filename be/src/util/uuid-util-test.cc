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

#include <cstring>
#include <string>

#include "testutil/gtest-util.h"
#include "util/uuid-util.h"

namespace impala {

static const uint8_t TEST_UUID_BYTES[UUID_BYTE_LEN] = {
    0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78,
    0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};

static const char* TEST_UUID_STRING = "12345678-1234-5678-1234-567812345678";

TEST(UuidUtil, UuidBytesToString) {
  char out[UUID_STRING_LEN];
  UuidBytesToString(TEST_UUID_BYTES, out);
  EXPECT_EQ(std::string(out, UUID_STRING_LEN), TEST_UUID_STRING);
}

TEST(UuidUtil, ParseCanonicalUuidStringToBytesValid) {
  uint8_t out[UUID_BYTE_LEN];
  EXPECT_TRUE(ParseCanonicalUuidStringToBytes(
      TEST_UUID_STRING, UUID_STRING_LEN, out));
  EXPECT_EQ(0, memcmp(out, TEST_UUID_BYTES, UUID_BYTE_LEN));
}

TEST(UuidUtil, ParseCanonicalUuidStringToBytesUppercaseHex) {
  const char* upper = "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB";
  uint8_t expected[UUID_BYTE_LEN] = {
      0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0xab, 0xcd,
      0xef, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0xab};
  uint8_t out[UUID_BYTE_LEN];
  EXPECT_TRUE(ParseCanonicalUuidStringToBytes(upper, UUID_STRING_LEN, out));
  EXPECT_EQ(0, memcmp(out, expected, UUID_BYTE_LEN));
}

TEST(UuidUtil, ParseAndFormatRoundTrip) {
  uint8_t parsed[UUID_BYTE_LEN];
  EXPECT_TRUE(ParseCanonicalUuidStringToBytes(
      TEST_UUID_STRING, UUID_STRING_LEN, parsed));

  char formatted[UUID_STRING_LEN];
  UuidBytesToString(parsed, formatted);
  EXPECT_EQ(std::string(formatted, UUID_STRING_LEN), TEST_UUID_STRING);
}

TEST(UuidUtil, ParseCanonicalUuidStringToBytesInvalidLength) {
  uint8_t out[UUID_BYTE_LEN];
  EXPECT_FALSE(ParseCanonicalUuidStringToBytes("short", 5, out));
  EXPECT_FALSE(ParseCanonicalUuidStringToBytes(
      TEST_UUID_STRING, UUID_STRING_LEN - 1, out));
}

TEST(UuidUtil, ParseCanonicalUuidStringToBytesInvalidDashPosition) {
  uint8_t out[UUID_BYTE_LEN];
  const char* bad_dash = "1234567811234-5678-1234-567812345678";
  EXPECT_FALSE(ParseCanonicalUuidStringToBytes(bad_dash, UUID_STRING_LEN, out));
}

TEST(UuidUtil, ParseCanonicalUuidStringToBytesInvalidCharacter) {
  uint8_t out[UUID_BYTE_LEN];
  const char* bad_hex = "12345678-1234-5678-1234-56781234567g";
  EXPECT_FALSE(ParseCanonicalUuidStringToBytes(bad_hex, UUID_STRING_LEN, out));
}

} // namespace impala
