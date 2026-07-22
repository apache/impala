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

#pragma once

#include <cstdint>

namespace impala {

/// Size of a canonical UUID string: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
static constexpr int UUID_STRING_LEN = 36;
/// Size of UUID raw byte representation
static constexpr int UUID_BYTE_LEN = 16;

inline int HexCharToNibble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

/// Converts 16 raw UUID bytes to canonical lowercase hex string (36 chars).
/// 'out' must have space for at least UUID_STRING_LEN bytes.
inline void UuidBytesToString(const uint8_t* bytes, char* out) {
  static const char hex[] = "0123456789abcdef";
  // Group sizes: 4-2-2-2-6 bytes = 8-4-4-4-12 hex chars with dashes
  static const int group_sizes[] = {4, 2, 2, 2, 6};
  int pos = 0;
  int byte_idx = 0;
  for (int g = 0; g < 5; ++g) {
    if (g > 0) out[pos++] = '-';
    for (int i = 0; i < group_sizes[g]; ++i) {
      out[pos++] = hex[(bytes[byte_idx] >> 4) & 0x0f];
      out[pos++] = hex[bytes[byte_idx] & 0x0f];
      ++byte_idx;
    }
  }
}

/// Parses a canonical UUID string into 16 raw bytes. Returns false on invalid input.
inline bool ParseCanonicalUuidStringToBytes(const char* str, int len, uint8_t* out) {
  if (len != UUID_STRING_LEN) return false;
  static const int dash_positions[] = {8, 13, 18, 23};
  int dash_idx = 0;
  int byte_idx = 0;
  int hi = -1;
  for (int i = 0; i < len; ++i) {
    if (dash_idx < 4 && i == dash_positions[dash_idx]) {
      if (str[i] != '-') return false;
      ++dash_idx;
      continue;
    }
    int nibble = HexCharToNibble(str[i]);
    if (nibble < 0) return false;
    if (hi < 0) {
      hi = nibble;
    } else {
      out[byte_idx++] = static_cast<uint8_t>((hi << 4) | nibble);
      hi = -1;
    }
  }
  return byte_idx == UUID_BYTE_LEN && hi < 0;
}

} // namespace impala
