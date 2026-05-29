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

#include <ostream>
#include <string>
#include <string_view>

#include "common/status.h"
#include "runtime/string-value.h"

namespace impala_udf {
class FunctionContext;
struct StringVal;
}

namespace impala {

// Physical type tags for the variant binary encoding.
// Corresponds to basic_type=0 (primitive) with type_info encoding the physical type.
enum class VariantPhysicalType : uint8_t {
  VNULL = 0,
  BOOLEAN_TRUE = 1,
  BOOLEAN_FALSE = 2,
  INT8 = 3,
  INT16 = 4,
  INT32 = 5,
  INT64 = 6,
  DOUBLE = 7,
  DECIMAL4 = 8,
  DECIMAL8 = 9,
  DECIMAL16 = 10,
  DATE = 11,
  TIMESTAMPTZ = 12,
  TIMESTAMPNTZ = 13,
  FLOAT = 14,
  BINARY = 15,
  STRING = 16,
  TIME = 17,
  TIMESTAMPTZ_NANOS = 18,
  TIMESTAMPNTZ_NANOS = 19,
  UUID = 20
};

inline std::ostream& operator<<(std::ostream& os, VariantPhysicalType pt) {
  switch (pt) {
    case VariantPhysicalType::VNULL: return os << "VNULL";
    case VariantPhysicalType::BOOLEAN_TRUE: return os << "BOOLEAN_TRUE";
    case VariantPhysicalType::BOOLEAN_FALSE: return os << "BOOLEAN_FALSE";
    case VariantPhysicalType::INT8: return os << "INT8";
    case VariantPhysicalType::INT16: return os << "INT16";
    case VariantPhysicalType::INT32: return os << "INT32";
    case VariantPhysicalType::INT64: return os << "INT64";
    case VariantPhysicalType::DOUBLE: return os << "DOUBLE";
    case VariantPhysicalType::DECIMAL4: return os << "DECIMAL4";
    case VariantPhysicalType::DECIMAL8: return os << "DECIMAL8";
    case VariantPhysicalType::DECIMAL16: return os << "DECIMAL16";
    case VariantPhysicalType::DATE: return os << "DATE";
    case VariantPhysicalType::TIMESTAMPTZ: return os << "TIMESTAMPTZ";
    case VariantPhysicalType::TIMESTAMPNTZ: return os << "TIMESTAMPNTZ";
    case VariantPhysicalType::FLOAT: return os << "FLOAT";
    case VariantPhysicalType::BINARY: return os << "BINARY";
    case VariantPhysicalType::STRING: return os << "STRING";
    case VariantPhysicalType::TIME: return os << "TIME";
    case VariantPhysicalType::TIMESTAMPTZ_NANOS: return os << "TIMESTAMPTZ_NANOS";
    case VariantPhysicalType::TIMESTAMPNTZ_NANOS: return os << "TIMESTAMPNTZ_NANOS";
    case VariantPhysicalType::UUID: return os << "UUID";
  }
  return os << "UNKNOWN(" << static_cast<int>(pt) << ")";
}

// Basic type tags encoded in bits 0-1 of the value header byte.
enum class VariantBasicType : uint8_t {
  PRIMITIVE = 0,
  SHORT_STRING = 1,
  OBJECT = 2,
  ARRAY = 3
};

inline std::ostream& operator<<(std::ostream& os, VariantBasicType bt) {
  switch (bt) {
    case VariantBasicType::PRIMITIVE: return os << "PRIMITIVE";
    case VariantBasicType::SHORT_STRING: return os << "SHORT_STRING";
    case VariantBasicType::OBJECT: return os << "OBJECT";
    case VariantBasicType::ARRAY: return os << "ARRAY";
  }
  return os << "UNKNOWN(" << static_cast<int>(bt) << ")";
}

// Parses and provides access to the variant metadata blob (field name dictionary).
// The metadata format:
//   - Header byte: version (bits 0-3), sorted flag (bit 4),
//     offset_size_minus_one (bits 6-7)
//   - Dictionary size: variable-width integer (1-4 bytes based on offset size)
//   - Offsets: (dict_size + 1) entries of offset_size bytes each
//   - String data: concatenated field name strings
class VariantMetadata {
 public:
  VariantMetadata() = default;

  // Initialize from a metadata blob. Returns error if the blob is malformed.
  Status Init(const uint8_t* data, uint32_t len);

  // Returns the number of field names in the dictionary.
  uint32_t DictionarySize() const { return dict_size_; }

  // Returns the field name at the given dictionary index.
  // index must be in [0, DictionarySize()).
  std::string_view GetFieldName(uint32_t index) const;

  // Looks up a field name in the dictionary. Returns the index if found, -1 otherwise.
  // If the dictionary is sorted, uses binary search.
  int FindFieldId(std::string_view name) const;

  bool IsValid() const { return offsets_ != nullptr; }

 private:
  uint32_t ReadOffset(uint32_t index) const;

  const uint8_t* offsets_ = nullptr;
  const uint8_t* string_data_ = nullptr;
  uint32_t dict_size_ = 0;
  uint8_t version_ = 0;
  uint8_t offset_size_ = 0;  // 1, 2, 3, or 4 bytes per offset
  bool is_sorted_ = false;
};

// Provides access to a variant value blob. Requires a VariantMetadata for field name
// lookups when navigating objects.
//
// Value format header byte:
//   bits 0-1: basic_type (PRIMITIVE=0, SHORT_STRING=1, OBJECT=2, ARRAY=3)
//   bits 2-7: type_info
//     - For PRIMITIVE: physical type id
//     - For SHORT_STRING: string length (0-63 bytes)
//     - For OBJECT: field_offset_size_minus_one (bits 2-3),
//                   field_id_size_minus_one (bits 4-5), is_large (bit 6)
//     - For ARRAY: offset_size_minus_one (bits 2-3), is_large (bit 4)
class VariantValue {
 public:
  VariantValue() = default;
  VariantValue(const uint8_t* data, int32_t len, const VariantMetadata* metadata)
      : data_(data), len_(len), metadata_(metadata) {}

  // Returns the basic type of this value.
  VariantBasicType GetBasicType() const;

  // Returns the physical type (only valid for PRIMITIVE basic type).
  VariantPhysicalType GetPhysicalType() const;

  // Returns true if this value is null.
  bool IsNull() const;

  // Scalar accessors. Caller must ensure the physical type matches.
  bool GetBoolean() const;
  int8_t GetInt8() const;
  int16_t GetInt16() const;
  int32_t GetInt32() const;
  int64_t GetInt64() const;
  float GetFloat() const;
  double GetDouble() const;
  StringValue GetString() const;
  StringValue GetBinary() const;

  // Object access.
  uint32_t GetObjectSize() const;
  // Gets the field value by field name. Returns false if field not found.
  bool GetFieldByName(std::string_view name, VariantValue* result) const;
  // Gets the field value by position index.
  bool GetFieldByIndex(uint32_t index, VariantValue* result) const;
  // Gets the field name at position index in this object.
  std::string_view GetFieldNameByIndex(uint32_t index) const;

  // Array access.
  uint32_t GetArraySize() const;
  bool GetArrayElement(uint32_t index, VariantValue* result) const;

  // Navigate a dotted path like "field.nested[0].value".
  // Returns false if the path cannot be resolved.
  bool NavigatePath(const std::string& path, VariantValue* result) const;

  // Serialize this variant value to JSON string.
  Status ToJson(std::string* json_out) const;
  Status ToJson(impala_udf::FunctionContext* ctx, impala_udf::StringVal* result) const;

  bool IsValid() const { return data_ != nullptr; }
  const uint8_t* Data() const { return data_; }
  uint32_t Len() const { return len_; }

  // Reads a primitive value of type T from the payload (data_ + offset).
  // Default offset is 1 (immediately after the header byte).
  template <typename T>
  T ReadValue(uint32_t offset = 1) const {
    DCHECK_GE(len_, offset + sizeof(T));
    T val;
    memcpy(&val, data_ + offset, sizeof(T));
    return val;
  }

 private:
  // Helper to read a variable-width unsigned integer.
  static uint32_t ReadUint(const uint8_t* data, uint32_t size);

  // For OBJECT: parse the header to get field count and internal layout.
  uint32_t ObjectFieldIdSize() const;
  uint32_t ObjectOffsetSize() const;
  uint32_t ObjectNumFields() const;
  const uint8_t* ObjectFieldIdsStart() const;
  const uint8_t* ObjectOffsetsStart() const;
  const uint8_t* ObjectDataStart() const;

  // For ARRAY: parse the header to get element count and offsets.
  uint32_t ArrayOffsetSize() const;
  uint32_t ArrayNumElements() const;
  const uint8_t* ArrayOffsetsStart() const;
  const uint8_t* ArrayDataStart() const;

  const uint8_t* data_ = nullptr;
  uint32_t len_ = 0;
  const VariantMetadata* metadata_ = nullptr;
};

}  // namespace impala
