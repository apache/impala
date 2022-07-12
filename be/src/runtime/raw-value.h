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

#include <iosfwd>
#include <string>

#include "codegen/impala-ir.h"
#include "runtime/collection-value.h"
#include "runtime/types.h"

namespace impala {

class MemPool;
class SlotDescriptor;
struct StringValue;
class Tuple;

/// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
 public:
  /// Ascii output precision for double/float, print 16 digits.
  static const int ASCII_PRECISION = 16;

  /// Single NaN values to ensure all NaN values can be assigned one bit pattern
  /// that will always compare and hash the same way.  Allows for all NaN values
  /// to be put into the same "group by" bucket.
  static constexpr double CANONICAL_DOUBLE_NAN = std::numeric_limits<double>::quiet_NaN();
  static constexpr float CANONICAL_FLOAT_NAN = std::numeric_limits<float>::quiet_NaN();
  /// The canonical zero values when comparing negative and positive zeros.
  static constexpr double CANONICAL_DOUBLE_ZERO = 0.0;
  static constexpr float CANONICAL_FLOAT_ZERO = 0.0f;

  /// Convert 'value' into ascii and write to 'stream'. NULL turns into "NULL". 'scale'
  /// determines how many digits after the decimal are printed for floating point numbers,
  /// -1 indicates to use the stream's current formatting. Doesn't support complex types.
  /// If 'quote_val' is true, write STRING, VARCHAR, CHAR, DATE, TIMESTAMP values in
  /// quoted form surrounded by double quotes.
  /// TODO: for string types, we just print the result regardless of whether or not it
  /// ascii. This could be undesirable.
  static void PrintValue(const void* value, const ColumnType& type, int scale,
                         std::stringstream* stream, bool quote_val=false);

  /// Write ascii value to string instead of stringstream.
  static void PrintValue(const void* value, const ColumnType& type, int scale,
                         std::string* str);

  /// Return ascii value string.
  static std::string PrintValue(const void* value, const ColumnType& type, int scale) {
    std::string str;
    PrintValue(value, type, scale, &str);
    return str;
  }

  /// Similar to PrintValue() but works with collection values.
  /// Converts 'coll_val' collection into ascii and writes to 'stream'.
  static void PrintCollectionValue(const CollectionValue* coll_val,
      const TupleDescriptor* item_tuple_desc, int scale, std::stringstream *stream,
      bool is_map);

  /// Writes the byte representation of a value to a stringstream character-by-character
  static void PrintValueAsBytes(const void* value, const ColumnType& type,
                                std::stringstream* stream);

  /// Returns hash value for 'v' interpreted as 'type'.  The resulting hash value
  /// is combined with the seed value. Inlined in IR so that the constant 'type' can be
  /// propagated.
  static uint32_t IR_ALWAYS_INLINE GetHashValue(
      const void* v, const ColumnType& type, uint32_t seed = 0) noexcept;

  /// Templatized version of GetHashValue, use if type is known ahead. GetHashValue
  /// handles nulls. Inlined in IR so that the constant 'type' can be propagated.
  template<typename T>
  static inline uint32_t IR_ALWAYS_INLINE GetHashValue(
      const T* v, const ColumnType& type, uint32_t seed = 0) noexcept;

  /// Returns hash value for non-nullable 'v' for type T. GetHashValueNonNull doesn't
  /// handle nulls.
  template<typename T>
  static inline uint32_t GetHashValueNonNull(const T* v, const ColumnType& type,
      uint32_t seed = 0);

  /// Get a 64-bit hash value using the FastHash function.
  /// https://code.google.com/archive/p/fast-hash/
  static uint64_t GetHashValueFastHash(const void* v, const ColumnType& type,
      uint64_t seed);

  /// Templatized version of GetHashValueFastHash, use if type is known ahead.
  /// GetHashValueFastHash handles nulls. Inlined in IR so that the constant
  /// 'type' can be propagated.
  template <typename T>
  static inline uint64_t IR_ALWAYS_INLINE GetHashValueFastHash(
      const T* v, const ColumnType& type, uint64_t seed);

  /// Returns hash value for non-nullable 'v' for type T. GetHashValueFastHashNonNull
  /// doesn't handle nulls.
  template <typename T>
  static inline uint64_t GetHashValueFastHashNonNull(
      const T* v, const ColumnType& type, uint64_t seed);

  // Get a 32-bit hash value using the FastHash algorithm.
  static uint32_t IR_ALWAYS_INLINE GetHashValueFastHash32(
      const void* v, const ColumnType& type, uint32_t seed = 0) noexcept;

  /// Compares both values.
  /// Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
  /// Inlined in IR so that the constant 'type' can be propagated.
  static int IR_ALWAYS_INLINE Compare(
      const void* v1, const void* v2, const ColumnType& type) noexcept;

  /// Writes the bytes of a given value into the slot of a tuple.
  /// For string values, the string data is copied into memory allocated from 'pool'
  /// only if pool is non-NULL.
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                    MemPool* pool);

  /// Writes 'src' into 'dst' for type.
  /// For string values, the string data is copied into 'pool' if pool is non-NULL.
  /// src must be non-NULL.
  static void Write(const void* src, void* dst, const ColumnType& type, MemPool* pool);

  /// Wrapper function for Write() to handle struct slots and its children. Additionally,
  /// gathers the string slots of the slot tree into 'string_values'.
  template <bool COLLECT_STRING_VALS>
  static void Write(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
    std::vector<StringValue*>* string_values);

  /// Returns true if v1 == v2.
  /// This is more performant than Compare() == 0 for string equality, mostly because of
  /// the length comparison check.
  static inline bool Eq(const void* v1, const void* v2, const ColumnType& type);

  /// Returns true if val/type correspond to a NaN floating point value.
  static inline bool IsNaN(const void* val, const ColumnType& type);

  /// Returns true if val/type correspond to a +0/-0 floating point value.
  static inline bool IsFloatingZero(const void* val, const ColumnType& type);

  /// Returns the canonical form of the given value. Currently this means a unified NaN
  /// value in case of NaN and +0 in case of +0/-0.
  static inline const void* CanonicalValue(const void* val, const ColumnType& type);

  /// Returns a canonical NaN value for a floating point type
  /// (which will always have the same bit-pattern to maintain consistency in hashing).
  static inline const void* CanonicalNaNValue(const ColumnType& type);

  // Returns positive zero for floating point types.
  static inline const void* PositiveFloatingZero(const ColumnType& type);

private:
  /// Recursive helper function for Write() to handle struct slots.
  template <bool COLLECT_STRING_VALS>
  static void WriteStruct(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);

  /// Gets the destination slot from 'tuple' and 'slot_desc', writes value to this slot
  /// using Write(). Collects pointer of the string slots to 'string_values'. 'slot_desc'
  /// has to be primitive type.
  template <bool COLLECT_STRING_VALS>
  static void WritePrimitive(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);
};
}
