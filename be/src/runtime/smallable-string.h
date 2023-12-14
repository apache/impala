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

#include <string>

#include "util/ubsan.h"

#include "common/logging.h"

namespace impala {

/// String implementation that can apply the Small String Optimization on an
/// on-demand basis: every SmallableString object starts with the representation
/// that uses an 8-byte pointer and a 4-byte length. But if users invoke the
/// Smallify() method on this object and the length is less than 'SMALL_LIMIT' then
/// the representation switches to the small string layout, i.e. a SMALL_LIMIT-size
/// buffer and a single byte to store the (small) length. An indicator bit is set
/// in the last byte so the methods of this class can know which representation is
/// being used. On little-endian architectures this is the most significant bit (MSB) of
/// both LONG_STRING.len and SMALL_STRING.len.
/// TODO: On big endian architectures this will be the least significant bit (LSB) of
/// both LONG_STRING.len and SMALL_STRING.len.
/// We can have the indicator bit in 'len' because StringValues are limited to 1GB
/// character data.
class __attribute__((__packed__)) SmallableString {
 public:
  static_assert(__BYTE_ORDER == __LITTLE_ENDIAN,
      "Current implementation of SmallableString assumes little-endianness");

  static constexpr int SMALL_LIMIT = 11;
  static constexpr unsigned char MSB_CHAR = 0b10000000;
  static constexpr unsigned char SMALLSTRING_MASK = 0b01111111;

  struct SimpleString {
    char* ptr;
    uint32_t len;
  };

  SmallableString() {
    rep.long_rep.ptr = nullptr;
    rep.long_rep.len = 0;
  }

  /// Constructs SmallableString based on 'other'. If 'other' uses the long
  /// representation, then so will the newly created SmallableString, and it will point
  /// to the same data. If 'other' uses the small representation, then newly created
  /// object will also be small.
  SmallableString(const SmallableString& other) {
    memcpy(this, &other, sizeof(*this));
  }

  /// Creates smallable string from 'ptr' and 'len'. It will use the long representation.
  SmallableString(char* ptr, int len) {
    // Invalid string values might have negative lengths. We also have backend tests
    // for this.
    if (UNLIKELY(len < 0)) len = 0;
    rep.long_rep.ptr = ptr;
    rep.long_rep.len = len;
    DCHECK(!IsSmall());
  }

  /// Construct a SmallableString from 's'. 's' must be valid for as long as
  /// this object is valid.
  explicit SmallableString(const std::string& s) :
      SmallableString(const_cast<char*>(s.c_str()), s.size()) {}

  /// Constructs a SmallableString from 's'. 's' must be valid for as long as
  /// this object is valid.
  /// s must be a null-terminated string. This constructor is to prevent
  /// accidental use of the version taking an std::string.
  explicit SmallableString(const char* s) :
      SmallableString(const_cast<char*>(s), strlen(s)) {}

  SmallableString& operator=(const SmallableString& other) {
    Assign(other);
    return *this;
  }

  void Assign(const SmallableString& other) {
    memcpy(this, &other, sizeof(*this));
  }

  /// Assigns 'ptr' and 'len' to this string's long representation. Negative 'len'
  /// is overwritten with 0.
  void Assign(char* ptr, int len) {
    // Invalid string values might have negative lengths. We also have backend tests
    // for this.
    if (UNLIKELY(len < 0)) len = 0;
    rep.long_rep.ptr = ptr;
    rep.long_rep.len = len;
    DCHECK(!IsSmall());
  }

  /// Assigns 'ptr' and 'len' to this string's long representation without any checks.
  void UnsafeAssign(char* ptr, int len) {
    rep.long_rep.ptr = ptr;
    rep.long_rep.len = len;
  }

  void Clear() { memset(this, 0, sizeof(*this)); }

  bool IsSmall() const {
    char last_char = reinterpret_cast<const char*>(this)[sizeof(*this) - 1];
    return last_char & MSB_CHAR;
  }

  bool Smallify() {
    if (IsSmall()) return true;
    if (rep.long_rep.len > SMALL_LIMIT) return false;
    char* s = rep.long_rep.ptr;
    uint32_t len = rep.long_rep.len;
    // Let's zero out the object so compression algorithms will be more efficient
    // on small strings (there will be no garbage between string data and len).
    memset(this, 0, sizeof(*this));
    Ubsan::MemCpy(rep.small_rep.buf, s, len);
    SetSmallLen(len);
    return true;
  }

  int Len() const {
    if (IsSmall()) {
      return GetSmallLen();
    } else {
      return rep.long_rep.len;
    }
  }

  char* Ptr() const {
    if (IsSmall()) {
      return const_cast<char*>(rep.small_rep.buf);
    } else {
      return rep.long_rep.ptr;
    }
  }

  /// Sets a new length for this object. To keep it safe, the length can only be
  /// decreased.
  void SetLen(int len) {
    DCHECK_LE(len, Len());
    if (IsSmall()) {
      SetSmallLen(len);
    } else {
      rep.long_rep.len = len;
      DCHECK(!IsSmall());
    }
  }

  void SetPtr(char* ptr) {
    DCHECK(!IsSmall());
    rep.long_rep.ptr = ptr;
  }

  SimpleString ToSimpleString() const {
    SimpleString ret;
    if (IsSmall()) {
      ret.ptr = const_cast<char*>(rep.small_rep.buf);
      ret.len = GetSmallLen();
    } else {
      ret.ptr = rep.long_rep.ptr;
      ret.len = rep.long_rep.len;
    }
    return ret;
  }

 private:
  int GetSmallLen() const {
    return rep.small_rep.len & SMALLSTRING_MASK;
  }

  void SetSmallLen(int len) {
    rep.small_rep.len = len;
    rep.small_rep.len |= MSB_CHAR;
  }

  struct SmallStringRep {
    char buf[SMALL_LIMIT];
    unsigned char len;
  };

  struct __attribute__((__packed__)) LongStringRep {
    /// TODO: change ptr to an offset relative to a contiguous memory block,
    /// so that we can send row batches between nodes without having to swizzle
    /// pointers
    char* ptr;
    uint32_t len;
  };

  static_assert(sizeof(SmallStringRep) == sizeof(LongStringRep),
      "sizeof(SmallStringRep) must be equal to sizeof(LongStringRep)");

  union {
    SmallStringRep small_rep;
    LongStringRep long_rep;
  } rep;
};

}

