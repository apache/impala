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
  uint32_t string_data_len_ = 0;  // number of bytes in the string-data region
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
  VariantValue(const uint8_t* data, uint32_t len, const VariantMetadata* metadata)
      : data_(data), len_(len), metadata_(metadata) {}

  // Returns the basic type of this value.
  VariantBasicType GetBasicType() const;

  // Returns the physical type (only valid for PRIMITIVE basic type).
  VariantPhysicalType GetPhysicalType() const;

  // Returns true if this value is null.
  bool IsNull() const;

  // Scalar accessors. Each returns false and leaves '*out' unchanged if the value is not
  // of the expected physical type or its payload would read out of bounds.
  [[nodiscard]] bool GetBoolean(bool* out) const;
  [[nodiscard]] bool GetInt8(int8_t* out) const;
  [[nodiscard]] bool GetInt16(int16_t* out) const;
  [[nodiscard]] bool GetInt32(int32_t* out) const;
  [[nodiscard]] bool GetInt64(int64_t* out) const;
  [[nodiscard]] bool GetFloat(float* out) const;
  [[nodiscard]] bool GetDouble(double* out) const;
  [[nodiscard]] bool GetString(StringValue* out) const;
  [[nodiscard]] bool GetBinary(StringValue* out) const;

  // Object access. Each returns false if this value is not a well-formed object (a
  // corrupt or truncated header/offset table), if 'index' is out of range, or if the
  // field's encoding is out of bounds.
  [[nodiscard]] bool GetObjectSize(uint32_t* out) const;
  // Gets the field value by field name. Returns false if the field is not found.
  [[nodiscard]] bool GetFieldByName(std::string_view name, VariantValue* result) const;
  // Gets the field value by position index.
  [[nodiscard]] bool GetFieldByIndex(uint32_t index, VariantValue* result) const;
  // Gets the field name at position index in this object.
  [[nodiscard]] bool GetFieldNameByIndex(uint32_t index, std::string_view* out) const;

  // Array access. Returns false on a corrupt/truncated array or an out-of-range index.
  [[nodiscard]] bool GetArraySize(uint32_t* out) const;
  [[nodiscard]] bool GetArrayElement(uint32_t index, VariantValue* result) const;

  // Navigate a dotted path like "field.nested[0].value".
  // Returns false if the path cannot be resolved.
  [[nodiscard]] bool NavigatePath(const std::string& path, VariantValue* result) const;

  // Serialize this variant value to JSON string.
  Status ToJson(std::string* json_out) const;
  Status ToJson(impala_udf::FunctionContext* ctx, impala_udf::StringVal* result) const;
  // Serialize this variant value as JSON directly into 'out'. The JSON is fully
  // buffered internally and only flushed to 'out' on success.
  Status ToJson(std::ostream* out) const;

  bool IsValid() const { return data_ != nullptr; }
  const uint8_t* Data() const { return data_; }
  uint32_t Len() const { return len_; }

  // Reads a primitive of type T from the payload at data_ + offset (offset defaults to 1,
  // just past the header byte). Returns false without modifying '*out' if the read would
  // extend past the end of the value buffer.
  template <typename T>
  [[nodiscard]] bool ReadValue(T* out, uint32_t offset = 1) const {
    if (offset > len_ || sizeof(T) > len_ - offset) return false;
    memcpy(out, data_ + offset, sizeof(T));
    return true;
  }

 private:
  // Helper to read a variable-width unsigned integer.
  static uint32_t ReadUint(const uint8_t* data, uint32_t size);

  // If this value is a PRIMITIVE with a readable header byte, sets '*pt' to its physical
  // type and returns true; otherwise returns false.
  [[nodiscard]] bool AsPrimitive(VariantPhysicalType* pt) const {
    if (data_ == nullptr || len_ < 1) return false;
    if (GetBasicType() != VariantBasicType::PRIMITIVE) return false;
    *pt = GetPhysicalType();
    return true;
  }

  // Checks this value is a PRIMITIVE of 'expected' physical type, then reads a T-sized
  // payload just past the header byte (see ReadValue()). Returns false without modifying
  // '*out' on a type mismatch or a truncated payload. Shared by the typed scalar getters.
  template <typename T>
  [[nodiscard]] bool ReadValueOfType(VariantPhysicalType expected, T* out) const {
    VariantPhysicalType pt;
    if (!AsPrimitive(&pt) || pt != expected) return false;
    return ReadValue(out);
  }

  // Validated internal layout of an object/array value. The pointers and lengths are
  // guaranteed to lie within the value buffer once the matching Parse*() returns true.
  struct ObjectLayout {
    uint32_t num_fields;
    const uint8_t* field_ids;
    const uint8_t* offsets;
    const uint8_t* data;
    uint32_t data_len;
  };
  struct ArrayLayout {
    uint32_t num_elems;
    const uint8_t* offsets;
    const uint8_t* data;
    uint32_t data_len;
  };
  // Parse and bounds-check the object/array header + offset table. Returns false (without
  // reading out of bounds) if the value is not that type or the header/tables do not fit.
  [[nodiscard]] bool ParseObjectLayout(ObjectLayout* out) const;
  [[nodiscard]] bool ParseArrayLayout(ArrayLayout* out) const;

  // Extract a single field/element from an already-parsed, validated layout, bounds-
  // checking the individual entry's offsets (and, for a field name, that its dictionary
  // id is in range). Shared by the public single-access accessors and by JSON
  // serialization, which parses the layout once and reuses it across the whole loop.
  [[nodiscard]] bool FieldFromLayout(const ObjectLayout& layout, uint32_t index,
      VariantValue* result) const;
  [[nodiscard]] bool FieldNameFromLayout(const ObjectLayout& layout, uint32_t index,
      std::string_view* out) const;
  [[nodiscard]] bool ElementFromLayout(const ArrayLayout& layout, uint32_t index,
      VariantValue* result) const;

  // JSON serialization lives in the .cc (so rapidjson stays out of this header) but needs
  // the private layout helpers above to parse each object/array once and reuse it.
  friend struct VariantJsonSerializer;

  // Bit-width helpers derived from the header byte (require a readable header).
  uint32_t ObjectFieldIdSize() const;
  uint32_t ObjectOffsetSize() const;
  uint32_t ArrayOffsetSize() const;

  const uint8_t* data_ = nullptr;
  uint32_t len_ = 0;
  const VariantMetadata* metadata_ = nullptr;
};

}  // namespace impala
