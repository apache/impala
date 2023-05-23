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
class StringValue;
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

  /// Writes the bytes of a given value into the slot of a tuple. Supports primitive and
  /// complex types. 'value' is allowed to be NULL. For string and collection values, the
  /// data is deep-copied into memory allocated from 'pool' if pool is non-NULL, otherwise
  /// the data is not copied.
  ///
  /// If COLLECT_VAR_LEN_VALS is true, gathers the non-NULL non-smallified string slots of
  /// the slot tree into 'string_values' and the non-NULL collection slots along with
  /// their byte sizes into 'collection_values' recursively. Smallified strings (see Small
  /// String Optimization, IMPALA-12373) are not collected. Children are placed before
  /// their parents in the vectors (post-order traversal) - see Tuple::MaterializeExprs()
  /// and Sorter::Run::CollectNonNullVarSlots() for the reason. If COLLECT_VAR_LEN_VALS is
  /// true, 'string_values' and 'collection_values' must be non-NULL.
  template <bool COLLECT_VAR_LEN_VALS>
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
      MemPool* pool, std::vector<StringValue*>* string_values,
      std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

  /// Convenience wrapper for the templated version with COLLECT_VAR_LEN_VALS=false.
  static void Write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
      MemPool* pool);

  /// Writes 'src' into 'dst' for the given primitive type. Does not support complex
  /// types. 'src' must be non-NULL. For string values, the string data is copied into
  /// 'pool' if pool is non-NULL.
  static void WriteNonNullPrimitive(const void* src, void* dst, const ColumnType& type,
      MemPool* pool);

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

  // Top level null values are printed as "NULL"; collections and structs are printed in
  // JSON format, which requires "null".
  static constexpr const char* NullLiteral(bool top_level) {
    return top_level ? "NULL" : "null";
  }

private:
  /// Like Write() but 'value' must be non-NULL.
  template <bool COLLECT_VAR_LEN_VALS>
  static void WriteNonNull(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values,
      std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

  /// Recursive helper function for Write() to handle struct slots.
  template <bool COLLECT_VAR_LEN_VALS>
  static void WriteStruct(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values,
      std::vector<std::pair<CollectionValue*, int64_t>>* collection_values);

  /// Recursive helper function for Write() to handle collection slots.
  template <bool COLLECT_VAR_LEN_VALS>
  static void WriteCollection(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values,
      vector<pair<CollectionValue*, int64_t>>* collection_values);

  template <bool COLLECT_VAR_LEN_VALS>
  static void WriteCollectionChildren(const CollectionValue& dest,
      const CollectionValue& src, const SlotDescriptor& collection_slot_desc,
      MemPool* pool, vector<StringValue*>* string_values,
      vector<pair<CollectionValue*, int64_t>>* collection_values);

  template <bool COLLECT_VAR_LEN_VALS>
  static void WriteCollectionVarlenChild(Tuple* child_dest_tuple, Tuple* child_src_tuple,
      const SlotDescriptor* slot_desc, MemPool* pool, vector<StringValue*>* string_values,
      vector<pair<CollectionValue*, int64_t>>* collection_values );

  /// Gets the destination slot from 'tuple' and 'slot_desc' and writes 'value' to this
  /// slot. 'value' must be primitive and non-NULL. If COLLECT_VAR_LEN_VALS is true,
  /// collects the pointers of string slots to 'string_values'.
  template <bool COLLECT_VAR_LEN_VALS>
  static void WritePrimitiveCollectVarlen(const void* value, Tuple* tuple,
      const SlotDescriptor* slot_desc, MemPool* pool,
      std::vector<StringValue*>* string_values);
};
}
