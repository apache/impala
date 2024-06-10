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


#ifndef IMPALA_RUNTIME_STRING_VALUE_H
#define IMPALA_RUNTIME_STRING_VALUE_H

#include <string.h>
#include <string>

#include "common/logging.h"
#include "udf/udf.h"
#include "util/hash-util.h"
#include "runtime/types.h"
#include "runtime/smallable-string.h"

namespace impala {

class StringValueTest;
class Tuple;

/// The format of a string-typed slot.
/// The returned StringValue of all functions that return StringValue
/// shares its buffer with the parent.
/// TODO: rename this to be less confusing with impala_udf::StringVal.
/// StringValues use SmallableStrings internally which adds on-demand
/// Small String Optimization.
class __attribute__((__packed__)) StringValue {
public:
  /// The current limitation for a string instance is 1GB character data.
  /// See IMPALA-1619 for more details.
  static constexpr int MAX_LENGTH = (1 << 30);

  using SimpleString = SmallableString::SimpleString;

  StringValue() {}
  StringValue(const StringValue& other): string_impl_(other.string_impl_) {}

  StringValue(char* ptr, int len): string_impl_(ptr, len) {
    DCHECK_GE(len, 0);
    DCHECK_LE(len, MAX_LENGTH);
  }

  /// Construct a StringValue from 's'. 's' must be valid for as long as
  /// this object is valid, unless 's' is short.
  explicit StringValue(const std::string& s)
    : string_impl_(s) {}

  /// Construct a StringValue from 's'. 's' must be valid for as long as
  /// this object is valid.
  /// s must be a null-terminated string.  This constructor is to prevent
  /// accidental use of the version taking an std::string.
  explicit StringValue(const char* s)
    : string_impl_(s) {}

  /// Only valid to call if source's length is small enough. Returns a StringValue object
  /// that is smallified.
  static StringValue MakeSmallStringFrom(const StringValue& source) {
    DCHECK_LE(source.Len(), SmallableString::SMALL_LIMIT);
    StringValue sv(source);
    sv.Smallify();
    DCHECK(sv.IsSmall());
    return sv;
  }

  void Assign(const StringValue& other) { string_impl_.Assign(other.string_impl_); }

  void Assign(char* ptr, int len) {
    string_impl_.Assign(ptr, len);
  }

  void UnsafeAssign(char* ptr, int len) {
    string_impl_.UnsafeAssign(ptr, len);
  }

  void Clear() { string_impl_.Clear(); }

  bool IsSmall() const { return string_impl_.IsSmall(); }

  int Len() const { return string_impl_.Len(); }

  /// Sets the length of this String object. Length can only be decreased.
  void SetLen(int len) { return string_impl_.SetLen(len); }

  char* Ptr() const { return string_impl_.Ptr(); }

  /// We can only call this if the string is not smallified.
  void SetPtr(char* ptr) { return string_impl_.SetPtr(ptr); }

  // START IR FUNCTIONS
  int IrLen() const;
  char* IrPtr() const;
  void IrSetLen(int len);
  void IrAssign(char* ptr, int len);
  void IrUnsafeAssign(char* ptr, int len);
  void IrClear();
  // END IR FUNCTIONS

  SimpleString ToSimpleString() const { return string_impl_.ToSimpleString(); }

  /// Byte-by-byte comparison. Returns:
  /// this < other: -1
  /// this == other: 0
  /// this > other: 1
  inline int Compare(const StringValue& other) const;

  /// ==
  inline bool Eq(const StringValue& other) const;
  inline bool operator==(const StringValue& other) const;
  /// !=
  inline bool Ne(const StringValue& other) const;
  inline bool operator!=(const StringValue& other) const;
  /// <=
  inline bool Le(const StringValue& other) const;
  inline bool operator<=(const StringValue& other) const;
  /// >=
  inline bool Ge(const StringValue& other) const;
  inline bool operator>=(const StringValue& other) const;
  /// <
  inline bool Lt(const StringValue& other) const;
  inline bool operator<(const StringValue& other) const;
  /// >
  inline bool Gt(const StringValue& other) const;
  inline bool operator>(const StringValue& other) const;

  std::string DebugString() const;

  /// Returns the substring starting at start_pos until the end of string.
  inline StringValue Substring(int start_pos) const;

  /// Returns the substring starting at start_pos with given length.
  /// If new_len < 0 then the substring from start_pos to end of string is returned. If
  /// new_len > len, len is extended to new_len.
  /// TODO: len should never be extended. This is not a trivial fix because UrlParser
  /// depends on the current behavior.
  inline StringValue Substring(int start_pos, int new_len) const;

  /// Trims leading and trailing spaces.
  inline StringValue Trim() const;

  void ToStringVal(impala_udf::StringVal* sv) const {
    *sv = impala_udf::StringVal(reinterpret_cast<uint8_t*>(Ptr()), Len());
  }

  // Treat up to first 8 bytes of the string as an 64-bit unsigned integer. If len is
  // less than 8, 8-len number of bytes of value '\0' are appended.
  uint64_t ToUInt64() const;

  static StringValue FromStringVal(const impala_udf::StringVal& sv) {
    return StringValue(reinterpret_cast<char*>(sv.ptr), sv.len);
  }

  /// Pads the end of the char pointer with spaces. num_chars is the number of used
  /// characters, cptr_len is the length of cptr
  inline static void PadWithSpaces(char* cptr, int64_t cptr_len, int64_t num_chars);

  /// Returns number of characters in a char array (ignores trailing spaces)
  inline static int64_t UnpaddedCharLength(const char* cptr, int64_t len);

  // Return the largest smaller string 'result' such that 'result' < 'this'. If no such
  // string exists, return an empty string. The smallest non-empty string is "\x00" and
  // the absolute smallest string is the empty string.
  std::string LargestSmallerString() const;

  // Return the least larger string 'result' such that 'this' < 'result'.
  std::string LeastLargerString() const;

  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

private:
  friend Tuple;
  friend StringValueTest;
  /// !!! THIS IS UNSAFE TO CALL ON EXISTING STRINGVALUE OBJECTS !!!
  /// Please make sure you only invoke it for newly created StringValues, e.g. on the
  /// target StringValue object of a deep copy operation.
  /// Tries to apply Small String Optimization if possible. Returns 'true' on success,
  /// 'false' otherwise. In the latter case the object remains unmodified.
  /// !!! THIS IS UNSAFE TO CALL ON EXISTING STRINGVALUE OBJECTS !!!
  bool Smallify() { return string_impl_.Smallify(); }

  SmallableString string_impl_;
};

/// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const StringValue& v) {
  return HashUtil::Hash(v.Ptr(), v.Len(), 0);
}

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

}

#endif
