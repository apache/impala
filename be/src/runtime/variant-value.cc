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

#include "runtime/variant-value.h"

#include <cstdio>
#include <cstring>
#include <limits>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/names.h"
#include "gutil/strings/substitute.h"
#include "runtime/date-parse-util.h"
#include "runtime/date-value.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "udf/udf.h"
#include "util/coding-util.h"

using impala::datetime_parse_util::SimpleDateFormatTokenizer;
using std::string_view;

namespace impala {

// --- VariantMetadata ---

Status VariantMetadata::Init(const uint8_t* data, uint32_t len) {
  if (len < 1) {
    return Status("Variant metadata blob is empty");
  }

  uint8_t header = data[0];
  version_ = header & 0x0F;
  if (version_ != 1) {
    return Status(Substitute(
        "Unsupported variant metadata version: $0", version_));
  }
  is_sorted_ = (header >> 4) & 0x01;
  offset_size_ = ((header >> 6) & 0x03) + 1;

  // All offset arithmetic below is done in 64-bit so that a corrupt (large) dict_size_
  // cannot overflow the bounds checks and let an under-sized blob pass validation.
  uint64_t pos = 1;
  if (pos + offset_size_ > len) {
    return Status("Variant metadata too short for dictionary size");
  }
  dict_size_ = 0;
  for (int i = 0; i < offset_size_; ++i) {
    dict_size_ |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
  }
  pos += offset_size_;

  // Offsets array: (dict_size_ + 1) entries of offset_size_ bytes each.
  offsets_ = data + pos;
  uint64_t offsets_len = (static_cast<uint64_t>(dict_size_) + 1) * offset_size_;
  if (pos + offsets_len > len) {
    return Status("Variant metadata too short for offset array");
  }
  pos += offsets_len;

  string_data_ = data + pos;
  // pos <= len is guaranteed by the check above, so this fits in uint32_t.
  string_data_len_ = static_cast<uint32_t>(len - pos);

  // Validate the dictionary offsets once here (rather than on every field-name access):
  // they must be non-decreasing and stay within the string-data region, so GetFieldName()
  // can never construct a string_view that points out of bounds.
  uint32_t prev = ReadOffset(0);
  for (uint32_t i = 1; i <= dict_size_; ++i) {
    uint32_t cur = ReadOffset(i);
    // Offsets must be non-decreasing. Equal consecutive offsets denote a zero-length
    // (empty-string) field name, e.g. the key in {"": 1}: that is valid JSON and is not
    // forbidden by the Variant spec, so only a strict decrease is rejected.
    if (cur < prev) {
      return Status("Variant metadata has non-monotonic dictionary offsets");
    }
    prev = cur;
  }
  if (prev > string_data_len_) {
    return Status("Variant metadata dictionary offset exceeds string data");
  }
  return Status::OK();
}

uint32_t VariantMetadata::ReadOffset(uint32_t index) const {
  static_assert(__BYTE_ORDER == __LITTLE_ENDIAN, "This code assumes little-endianness");
  const uint8_t* p = offsets_ + index * offset_size_;
  uint32_t val = 0;

  switch (offset_size_) {
    case 4:
      std::memcpy(&val, p, 4);
      return val;
    case 2:
      std::memcpy(&val, p, 2);
      return val;
    case 1:
      return *p;
    case 3:
      std::memcpy(&val, p, 3);
      return val;
    default:
      return 0;
  }
}

string_view VariantMetadata::GetFieldName(uint32_t index) const {
  // The caller (FieldNameFromLayout) must bound-check the index.
  DCHECK_LT(index, dict_size_);
  uint32_t start = ReadOffset(index);
  uint32_t end = ReadOffset(index + 1);
  // Guaranteed by the offset validation in Init().
  DCHECK_LE(start, end);
  DCHECK_LE(end, string_data_len_);
  return string_view(reinterpret_cast<const char*>(string_data_ + start),
      end - start);
}

// --- VariantValue ---

uint32_t VariantValue::ReadUint(const uint8_t* data, uint32_t size) {
  static_assert(__BYTE_ORDER == __LITTLE_ENDIAN, "This code assumes little-endianness");
  uint32_t val = 0;
  switch (size) {
    case 4:
      std::memcpy(&val, data, 4);
      return val;
    case 2:
      std::memcpy(&val, data, 2);
      return val;
    case 1:
      return *data;
    case 3:
      std::memcpy(&val, data, 3);
      return val;
    default:
      return 0;
  }
}

VariantBasicType VariantValue::GetBasicType() const {
  DCHECK(data_ != nullptr);
  return static_cast<VariantBasicType>(data_[0] & 0x03);
}

VariantPhysicalType VariantValue::GetPhysicalType() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::PRIMITIVE);
  return static_cast<VariantPhysicalType>((data_[0] >> 2) & 0x3F);
}

bool VariantValue::IsNull() const {
  return GetBasicType() == VariantBasicType::PRIMITIVE
      && GetPhysicalType() == VariantPhysicalType::VNULL;
}

bool VariantValue::GetBoolean(bool* out) const {
  VariantPhysicalType pt;
  if (!AsPrimitive(&pt)) return false;
  if (pt == VariantPhysicalType::BOOLEAN_TRUE) { *out = true; return true; }
  if (pt == VariantPhysicalType::BOOLEAN_FALSE) { *out = false; return true; }
  return false;
}

bool VariantValue::GetInt8(int8_t* out) const {
  return ReadValueOfType(VariantPhysicalType::INT8, out);
}

bool VariantValue::GetInt16(int16_t* out) const {
  return ReadValueOfType(VariantPhysicalType::INT16, out);
}

bool VariantValue::GetInt32(int32_t* out) const {
  return ReadValueOfType(VariantPhysicalType::INT32, out);
}

bool VariantValue::GetInt64(int64_t* out) const {
  return ReadValueOfType(VariantPhysicalType::INT64, out);
}

bool VariantValue::GetFloat(float* out) const {
  return ReadValueOfType(VariantPhysicalType::FLOAT, out);
}

bool VariantValue::GetDouble(double* out) const {
  return ReadValueOfType(VariantPhysicalType::DOUBLE, out);
}

bool VariantValue::GetString(StringValue* out) const {
  if (data_ == nullptr || len_ < 1) return false;
  VariantBasicType bt = GetBasicType();
  if (bt == VariantBasicType::SHORT_STRING) {
    uint32_t str_len = (data_[0] >> 2) & 0x3F;
    if (str_len > len_ - 1) return false;
    out->Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 1)), str_len);
    return true;
  }
  if (bt != VariantBasicType::PRIMITIVE
      || GetPhysicalType() != VariantPhysicalType::STRING || len_ < 5) {
    return false;
  }
  uint32_t str_len = ReadUint(data_ + 1, 4);
  if (str_len > len_ - 5) return false;
  out->Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 5)), str_len);
  return true;
}

bool VariantValue::GetBinary(StringValue* out) const {
  VariantPhysicalType pt;
  if (!AsPrimitive(&pt) || pt != VariantPhysicalType::BINARY || len_ < 5) return false;
  uint32_t bin_len = ReadUint(data_ + 1, 4);
  if (bin_len > len_ - 5) return false;
  out->Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 5)), bin_len);
  return true;
}

// --- Object accessors ---

uint32_t VariantValue::ObjectFieldIdSize() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::OBJECT);
  return ((data_[0] >> 4) & 0x03) + 1;
}

uint32_t VariantValue::ObjectOffsetSize() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::OBJECT);
  return ((data_[0] >> 2) & 0x03) + 1;
}

bool VariantValue::ParseObjectLayout(ObjectLayout* out) const {
  if (data_ == nullptr || len_ < 1) return false;
  if (GetBasicType() != VariantBasicType::OBJECT) return false;
  bool is_large = (data_[0] >> 6) & 0x01;
  uint32_t num_size = is_large ? 4 : 1;
  if (len_ < 1 + num_size) return false;
  uint32_t num_fields = ReadUint(data_ + 1, num_size);
  uint32_t field_id_size = ObjectFieldIdSize();
  uint32_t offset_size = ObjectOffsetSize();
  // Layout: header | field_ids[num_fields] | offsets[num_fields + 1] | data.
  // 64-bit math so a corrupt (large) num_fields cannot overflow the bound check.
  uint64_t data_start_off = static_cast<uint64_t>(1) + num_size
      + static_cast<uint64_t>(num_fields) * field_id_size
      + (static_cast<uint64_t>(num_fields) + 1) * offset_size;
  if (data_start_off > len_) return false;
  out->num_fields = num_fields;
  out->field_ids = data_ + 1 + num_size;
  out->offsets = out->field_ids + static_cast<uint64_t>(num_fields) * field_id_size;
  out->data = data_ + data_start_off;
  // The last offset is the total size of the value data (spec: "the byte after the end
  // of the last value"). Values are bounded by it rather than by the end of the buffer;
  // bytes after it (e.g. padding written by Hive) are ignored.
  uint32_t values_len = ReadUint(
      out->offsets + static_cast<uint64_t>(num_fields) * offset_size, offset_size);
  if (values_len > len_ - data_start_off) return false;
  out->data_len = values_len;
  return true;
}

bool VariantValue::GetObjectSize(uint32_t* out) const {
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return false;
  *out = layout.num_fields;
  return true;
}

VariantLookupResult VariantValue::GetFieldByName(string_view name,
    VariantValue* result) const {
  if (data_ == nullptr || len_ < 1 || metadata_ == nullptr) {
    return VariantLookupResult::CORRUPT;
  }
  if (GetBasicType() != VariantBasicType::OBJECT) return VariantLookupResult::NOT_FOUND;
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return VariantLookupResult::CORRUPT;
  uint32_t index;
  VariantLookupResult r = FindField(layout, name, &index);
  if (r != VariantLookupResult::FOUND) return r;
  return FieldFromLayout(layout, index, result) ? VariantLookupResult::FOUND
                                                : VariantLookupResult::CORRUPT;
}

VariantLookupResult VariantValue::FindField(const ObjectLayout& layout,
    string_view name, uint32_t* index) const {
  // Search this object's field ids by their names instead of looking up 'name' in the
  // dictionary first: with sorted_strings=0 the dictionary may hold a string more than
  // once and the object may use any of those ids. Like parquet-java, scan small objects
  // and binary search large ones.
  constexpr uint32_t BINARY_SEARCH_THRESHOLD = 32;
  string_view field_name;
  if (layout.num_fields >= BINARY_SEARCH_THRESHOLD) {
    // The field ids are sorted by name in unsigned byte order, which is also how
    // string_view::compare() orders chars.
    uint32_t lo = 0;
    uint32_t hi = layout.num_fields;
    while (lo < hi) {
      uint32_t mid = lo + (hi - lo) / 2;
      if (!FieldNameFromLayout(layout, mid, &field_name)) {
        return VariantLookupResult::CORRUPT;
      }
      int cmp = field_name.compare(name);
      if (cmp == 0) {
        *index = mid;
        return VariantLookupResult::FOUND;
      }
      if (cmp < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    // Spark before 4.3 (SPARK-58949) and parquet-java (GH-3735) sorted the field ids by
    // UTF-16 code units instead. The two orders differ only between code points in
    // U+E000..U+FFFF and above U+FFFF, which UTF-8 encodes with a leading byte >= 0xEE.
    // A name without such a byte compares the same in both orders, so the search above
    // finds it. Scan for the others.
    bool may_differ_in_utf16 = false;
    for (char c : name) {
      if (static_cast<uint8_t>(c) >= 0xEE) {
        may_differ_in_utf16 = true;
        break;
      }
    }
    if (!may_differ_in_utf16) return VariantLookupResult::NOT_FOUND;
  }
  for (uint32_t i = 0; i < layout.num_fields; ++i) {
    if (!FieldNameFromLayout(layout, i, &field_name)) {
      return VariantLookupResult::CORRUPT;
    }
    if (field_name == name) {
      *index = i;
      return VariantLookupResult::FOUND;
    }
  }
  return VariantLookupResult::NOT_FOUND;
}

bool VariantValue::SliceValue(const uint8_t* data, uint32_t data_len, uint32_t start,
    VariantValue* result) const {
  if (start >= data_len) return false;
  uint32_t size;
  if (!ValueSize(data + start, data_len - start, &size)) return false;
  *result = VariantValue(data + start, size, metadata_);
  return true;
}

bool VariantValue::FieldFromLayout(const ObjectLayout& layout, uint32_t index,
    VariantValue* result) const {
  if (index >= layout.num_fields) return false;
  uint32_t offset_size = ObjectOffsetSize();
  uint32_t start = ReadUint(layout.offsets + index * offset_size, offset_size);
  // Offsets are listed in field name order, but the values may be stored in any order
  // (Spark stores them in document order), so offset[index + 1] is not where this value
  // ends. Size the value from its own header, like parquet-java does.
  return SliceValue(layout.data, layout.data_len, start, result);
}

bool VariantValue::GetFieldByIndex(uint32_t index, VariantValue* result) const {
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return false;
  return FieldFromLayout(layout, index, result);
}

bool VariantValue::FieldNameFromLayout(const ObjectLayout& layout, uint32_t index,
    string_view* out) const {
  if (index >= layout.num_fields) return false;
  uint32_t field_id_size = ObjectFieldIdSize();
  uint32_t fid = ReadUint(layout.field_ids + index * field_id_size, field_id_size);
  uint32_t dict_size = (metadata_ != nullptr) ? metadata_->DictionarySize() : 0;
  if (fid >= dict_size) return false;
  *out = metadata_->GetFieldName(fid);
  return true;
}

bool VariantValue::GetFieldNameByIndex(uint32_t index, string_view* out) const {
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return false;
  return FieldNameFromLayout(layout, index, out);
}

// --- Array accessors ---

uint32_t VariantValue::ArrayOffsetSize() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::ARRAY);
  return ((data_[0] >> 2) & 0x03) + 1;
}

bool VariantValue::ParseArrayLayout(ArrayLayout* out) const {
  if (data_ == nullptr || len_ < 1) return false;
  if (GetBasicType() != VariantBasicType::ARRAY) return false;
  bool is_large = (data_[0] >> 4) & 0x01;
  uint32_t num_size = is_large ? 4 : 1;
  if (len_ < 1 + num_size) return false;
  uint32_t num_elems = ReadUint(data_ + 1, num_size);
  uint32_t offset_size = ArrayOffsetSize();
  // Layout: header | offsets[num_elems + 1] | data.
  uint64_t data_start_off = static_cast<uint64_t>(1) + num_size
      + (static_cast<uint64_t>(num_elems) + 1) * offset_size;
  if (data_start_off > len_) return false;
  out->num_elems = num_elems;
  out->offsets = data_ + 1 + num_size;
  out->data = data_ + data_start_off;
  // The last offset is the total size of the element data, see ParseObjectLayout().
  uint32_t values_len = ReadUint(
      out->offsets + static_cast<uint64_t>(num_elems) * offset_size, offset_size);
  if (values_len > len_ - data_start_off) return false;
  out->data_len = values_len;
  return true;
}

bool VariantValue::GetArraySize(uint32_t* out) const {
  ArrayLayout layout;
  if (!ParseArrayLayout(&layout)) return false;
  *out = layout.num_elems;
  return true;
}

bool VariantValue::ElementFromLayout(const ArrayLayout& layout, uint32_t index,
    VariantValue* result) const {
  if (index >= layout.num_elems) return false;
  uint32_t offset_size = ArrayOffsetSize();
  uint32_t start = ReadUint(layout.offsets + index * offset_size, offset_size);
  // The spec defines an element's offset only as where it starts, so size it from its
  // own header like an object field.
  return SliceValue(layout.data, layout.data_len, start, result);
}

VariantLookupResult VariantValue::GetArrayElement(uint32_t index,
    VariantValue* result) const {
  if (data_ == nullptr || len_ < 1) return VariantLookupResult::CORRUPT;
  if (GetBasicType() != VariantBasicType::ARRAY) return VariantLookupResult::NOT_FOUND;
  ArrayLayout layout;
  if (!ParseArrayLayout(&layout)) return VariantLookupResult::CORRUPT;
  if (index >= layout.num_elems) return VariantLookupResult::NOT_FOUND;
  return ElementFromLayout(layout, index, result) ? VariantLookupResult::FOUND
                                                  : VariantLookupResult::CORRUPT;
}

// --- Value sizing ---

bool VariantValue::ValueSize(const uint8_t* data, uint32_t avail, uint32_t* size) {
  if (data == nullptr || avail < 1) return false;
  // 64-bit, so that 5 + a long string's 4-byte length cannot wrap around. Initialized
  // because the basic type switch below has no default case.
  uint64_t sz = 0;
  switch (static_cast<VariantBasicType>(data[0] & 0x03)) {
    case VariantBasicType::SHORT_STRING:
      sz = 1 + (data[0] >> 2);
      break;
    case VariantBasicType::OBJECT: {
      VariantValue v(data, avail, nullptr);
      ObjectLayout layout;
      if (!v.ParseObjectLayout(&layout)) return false;
      sz = static_cast<uint64_t>(layout.data - data) + layout.data_len;
      break;
    }
    case VariantBasicType::ARRAY: {
      VariantValue v(data, avail, nullptr);
      ArrayLayout layout;
      if (!v.ParseArrayLayout(&layout)) return false;
      sz = static_cast<uint64_t>(layout.data - data) + layout.data_len;
      break;
    }
    case VariantBasicType::PRIMITIVE:
      switch (static_cast<VariantPhysicalType>(data[0] >> 2)) {
        case VariantPhysicalType::VNULL:
        case VariantPhysicalType::BOOLEAN_TRUE:
        case VariantPhysicalType::BOOLEAN_FALSE:
          sz = 1;
          break;
        case VariantPhysicalType::INT8:
          sz = 2;
          break;
        case VariantPhysicalType::INT16:
          sz = 3;
          break;
        case VariantPhysicalType::INT32:
        case VariantPhysicalType::DATE:
        case VariantPhysicalType::FLOAT:
          sz = 5;
          break;
        case VariantPhysicalType::INT64:
        case VariantPhysicalType::DOUBLE:
        case VariantPhysicalType::TIMESTAMPTZ:
        case VariantPhysicalType::TIMESTAMPNTZ:
        case VariantPhysicalType::TIME:
        case VariantPhysicalType::TIMESTAMPTZ_NANOS:
        case VariantPhysicalType::TIMESTAMPNTZ_NANOS:
          sz = 9;
          break;
        case VariantPhysicalType::DECIMAL4:
          sz = 6;
          break;
        case VariantPhysicalType::DECIMAL8:
          sz = 10;
          break;
        case VariantPhysicalType::DECIMAL16:
          sz = 18;
          break;
        case VariantPhysicalType::UUID:
          sz = 17;
          break;
        case VariantPhysicalType::BINARY:
        case VariantPhysicalType::STRING:
          if (avail < 5) return false;
          sz = 5 + static_cast<uint64_t>(ReadUint(data + 1, 4));
          break;
        default:
          return false;  // Unknown primitive type id (21-63).
      }
      break;
  }
  if (sz > avail) return false;
  *size = static_cast<uint32_t>(sz);
  return true;
}

// --- Path navigation ---

VariantLookupResult VariantValue::NavigatePath(std::string_view path,
    VariantValue* result) const {
  constexpr VariantLookupResult NOT_FOUND = VariantLookupResult::NOT_FOUND;
  // Fields and elements are sized from their headers when they are looked up. Size the
  // root the same way, so that a root that cannot be read (e.g. an empty or truncated
  // value, or an unknown type id) is corrupt too. Bytes after it (e.g. padding) are
  // ignored.
  uint32_t root_size;
  if (!ValueSize(data_, len_, &root_size)) return VariantLookupResult::CORRUPT;
  *result = *this;
  if (path == "$") return VariantLookupResult::FOUND;

  const char* p = path.data();
  const char* end = p + path.size();

  // Path must start with '$'.
  if (p >= end || *p != '$') return NOT_FOUND;
  ++p;

  // '$' alone is handled above; after '$' we need '.' or '['.
  if (p >= end) return NOT_FOUND;
  if (*p != '.' && *p != '[') return NOT_FOUND;
  if (*p == '.') ++p;

  // Must have at least one segment after the prefix.
  if (p >= end) return NOT_FOUND;

  while (p < end) {
    if (*p == '[') {
      ++p;
      // Require at least one digit.
      const char* digits_start = p;
      uint64_t index = 0;
      while (p < end && *p >= '0' && *p <= '9') {
        index = index * 10 + (*p - '0');
        // An array holds at most UINT32_MAX elements.
        if (index > std::numeric_limits<uint32_t>::max()) return NOT_FOUND;
        ++p;
      }
      if (p == digits_start) return NOT_FOUND;
      if (p >= end || *p != ']') return NOT_FOUND;
      ++p;  // skip ']'
      VariantLookupResult r =
          result->GetArrayElement(static_cast<uint32_t>(index), result);
      if (r != VariantLookupResult::FOUND) return r;
      if (p < end && *p == '.') ++p;
    } else {
      const char* seg_start = p;
      while (p < end && *p != '.' && *p != '[') ++p;
      int seg_len = p - seg_start;
      if (seg_len == 0) return NOT_FOUND;
      VariantLookupResult r =
          result->GetFieldByName(string_view(seg_start, seg_len), result);
      if (r != VariantLookupResult::FOUND) return r;
      if (p < end && *p == '.') ++p;
    }
  }
  return VariantLookupResult::FOUND;
}

// --- JSON serialization ---

using JsonWriter = rapidjson::Writer<rapidjson::StringBuffer>;

// Builds the error status returned when a variant value cannot be decoded because it is
// truncated or its payload would read out of bounds. 'what' names the offending part,
// e.g. "INT32" or "object field name".
static Status MalformedVariant(const char* what) {
  return Status(Substitute("Malformed variant $0", what));
}

// Serializes a VariantValue to JSON. A friend of VariantValue (declared in the header) so
// it can parse each object/array layout once via the private layout helpers and reuse it
// across the field/element loop, while keeping rapidjson out of variant-value.h.
struct VariantJsonSerializer {
  static Status Write(const VariantValue& val, const VariantMetadata& metadata,
      JsonWriter* writer, int depth = ColumnType::MAX_NESTING_DEPTH);
};

Status VariantJsonSerializer::Write(const VariantValue& val,
    const VariantMetadata& metadata, JsonWriter* writer, int depth) {
  if (UNLIKELY(depth <= 0)) {
    return Status("Variant value nesting exceeds the maximum allowed depth");
  }
  if (UNLIKELY(!val.IsValid() || val.Len() < 1)) {
    return MalformedVariant("value: empty or truncated value buffer");
  }
  switch (val.GetBasicType()) {
    case VariantBasicType::SHORT_STRING: {
      StringValue sv;
      if (UNLIKELY(!val.GetString(&sv))) {
        return MalformedVariant("short string");
      }
      writer->String(sv.Ptr(), sv.Len());
      return Status::OK();
    }
    case VariantBasicType::PRIMITIVE: {
      switch (val.GetPhysicalType()) {
        case VariantPhysicalType::VNULL:
          writer->Null();
          break;
        case VariantPhysicalType::BOOLEAN_TRUE:
          writer->Bool(true);
          break;
        case VariantPhysicalType::BOOLEAN_FALSE:
          writer->Bool(false);
          break;
        case VariantPhysicalType::INT8: {
          int8_t v;
          if (UNLIKELY(!val.GetInt8(&v))) return MalformedVariant("INT8");
          writer->Int(v);
          break;
        }
        case VariantPhysicalType::INT16: {
          int16_t v;
          if (UNLIKELY(!val.GetInt16(&v))) return MalformedVariant("INT16");
          writer->Int(v);
          break;
        }
        case VariantPhysicalType::INT32: {
          int32_t v;
          if (UNLIKELY(!val.GetInt32(&v))) return MalformedVariant("INT32");
          writer->Int(v);
          break;
        }
        case VariantPhysicalType::INT64: {
          int64_t v;
          if (UNLIKELY(!val.GetInt64(&v))) return MalformedVariant("INT64");
          writer->Int64(v);
          break;
        }
        case VariantPhysicalType::FLOAT: {
          float v;
          if (UNLIKELY(!val.GetFloat(&v))) return MalformedVariant("FLOAT");
          char buf[24];
          int n = snprintf(buf, sizeof(buf), "%g", v);
          writer->RawValue(buf, n, rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DOUBLE: {
          double v;
          if (UNLIKELY(!val.GetDouble(&v))) return MalformedVariant("DOUBLE");
          writer->Double(v);
          break;
        }
        case VariantPhysicalType::STRING: {
          StringValue sv;
          if (UNLIKELY(!val.GetString(&sv))) return MalformedVariant("STRING");
          writer->String(sv.Ptr(), sv.Len());
          break;
        }
        case VariantPhysicalType::DATE: {
          int32_t days;
          if (UNLIKELY(!val.ReadValue(&days))) return MalformedVariant("DATE");
          DateValue dv(static_cast<int64_t>(days));
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN];
          int n = DateParser::FormatDefault(dv, buf);
          if (UNLIKELY(n <= 0)) return Status("Variant DATE value out of range");
          DCHECK_LE(n, sizeof(buf));
          writer->String(buf, n);
          break;
        }
        case VariantPhysicalType::DECIMAL4: {
          int32_t unscaled;
          if (UNLIKELY(!val.ReadValue(&unscaled, 2))) {
            return MalformedVariant("DECIMAL4");
          }
          int scale = val.Data()[1];
          if (UNLIKELY(scale > ColumnType::MAX_DECIMAL4_PRECISION)) {
            return MalformedVariant("DECIMAL4: scale out of range");
          }
          string s = Decimal4Value(unscaled).ToString(
              ColumnType::MAX_DECIMAL4_PRECISION, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DECIMAL8: {
          int64_t unscaled;
          if (UNLIKELY(!val.ReadValue(&unscaled, 2))) {
            return MalformedVariant("DECIMAL8");
          }
          int scale = val.Data()[1];
          if (UNLIKELY(scale > ColumnType::MAX_DECIMAL8_PRECISION)) {
            return MalformedVariant("DECIMAL8: scale out of range");
          }
          string s = Decimal8Value(unscaled).ToString(
              ColumnType::MAX_DECIMAL8_PRECISION, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DECIMAL16: {
          __int128_t unscaled;
          if (UNLIKELY(!val.ReadValue(&unscaled, 2))) {
            return MalformedVariant("DECIMAL16");
          }
          int scale = val.Data()[1];
          if (UNLIKELY(scale > ColumnType::MAX_PRECISION)) {
            return MalformedVariant("DECIMAL16: scale out of range");
          }
          string s = Decimal16Value(unscaled).ToString(
              ColumnType::MAX_PRECISION, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::TIMESTAMPNTZ: {
          int64_t micros;
          if (UNLIKELY(!val.ReadValue(&micros))) {
            return MalformedVariant("TIMESTAMP");
          }
          TimestampValue ts = TimestampValue::UtcFromUnixTimeMicros(micros);
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN];
          int n = TimestampParser::FormatDefault(ts.date(), ts.time(), buf);
          if (UNLIKELY(n <= 0)) return Status("Variant TIMESTAMP value out of range");
          DCHECK_LE(n, sizeof(buf));
          writer->String(buf, n);
          break;
        }
        case VariantPhysicalType::TIMESTAMPNTZ_NANOS: {
          int64_t nanos;
          if (UNLIKELY(!val.ReadValue(&nanos))) {
            return MalformedVariant("TIMESTAMP");
          }
          TimestampValue ts =
              TimestampValue::UtcFromUnixTimeLimitedRangeNanos(nanos);
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN];
          int n = TimestampParser::FormatDefault(ts.date(), ts.time(), buf);
          if (UNLIKELY(n <= 0)) return Status("Variant TIMESTAMP value out of range");
          DCHECK_LE(n, sizeof(buf));
          writer->String(buf, n);
          break;
        }
        case VariantPhysicalType::BINARY: {
          StringValue sv;
          if (UNLIKELY(!val.GetBinary(&sv))) return MalformedVariant("BINARY");
          int64_t out_max;
          if (UNLIKELY(!Base64EncodeBufLen(sv.Len(), &out_max))) {
            return Status("Variant BINARY value too large to encode");
          }
          string encoded(out_max, '\0');
          unsigned out_len;
          Base64Encode(sv.Ptr(), sv.Len(), out_max, encoded.data(), &out_len);
          writer->String(encoded.data(), out_len);
          break;
        }
        case VariantPhysicalType::TIMESTAMPTZ:
        case VariantPhysicalType::TIME:
        case VariantPhysicalType::TIMESTAMPTZ_NANOS:
        case VariantPhysicalType::UUID:
          // TODO: implement proper formatting for these types.
          writer->String("<unsupported-type>");
          break;
        default:
          return Status(Substitute(
              "Malformed variant value: unknown primitive type id $0",
              static_cast<int>(val.GetPhysicalType())));
      }
      return Status::OK();
    }
    case VariantBasicType::OBJECT: {
      // Parse and bounds-check the header + offset table once, then reuse the layout for
      // every field rather than re-parsing on each accessor call.
      VariantValue::ObjectLayout layout;
      if (UNLIKELY(!val.ParseObjectLayout(&layout))) {
        return MalformedVariant("object");
      }
      writer->StartObject();
      for (uint32_t i = 0; i < layout.num_fields; ++i) {
        string_view field_name;
        if (UNLIKELY(!val.FieldNameFromLayout(layout, i, &field_name))) {
          return MalformedVariant("object field name");
        }
        writer->Key(field_name.data(), field_name.size());
        VariantValue child;
        if (UNLIKELY(!val.FieldFromLayout(layout, i, &child))) {
          return MalformedVariant("object field value (out of bounds or unknown type)");
        }
        RETURN_IF_ERROR(Write(child, metadata, writer, depth - 1));
      }
      writer->EndObject();
      return Status::OK();
    }
    case VariantBasicType::ARRAY: {
      VariantValue::ArrayLayout layout;
      if (UNLIKELY(!val.ParseArrayLayout(&layout))) {
        return MalformedVariant("array");
      }
      writer->StartArray();
      for (uint32_t i = 0; i < layout.num_elems; ++i) {
        VariantValue elem;
        if (UNLIKELY(!val.ElementFromLayout(layout, i, &elem))) {
          return MalformedVariant("array element (out of bounds or unknown type)");
        }
        RETURN_IF_ERROR(Write(elem, metadata, writer, depth - 1));
      }
      writer->EndArray();
      return Status::OK();
    }
  }
  return Status("Unknown variant basic type");
}

inline rapidjson::StringBuffer CreateStringBuffer(size_t expected_size) {
  constexpr auto default_capacity = rapidjson::StringBuffer::kDefaultCapacity;
  size_t capacity = max(expected_size, default_capacity);
  return {/*allocator=*/nullptr, capacity};
}

Status VariantValue::ToJson(std::string* json_out) const {
  DCHECK(metadata_ != nullptr);
  auto buffer = CreateStringBuffer(Len() * 2);
  JsonWriter writer(buffer);
  RETURN_IF_ERROR(VariantJsonSerializer::Write(
      *this, *metadata_, &writer));
  json_out->assign(buffer.GetString(), buffer.GetSize());
  return Status::OK();
}

Status VariantValue::ToJson(impala_udf::FunctionContext* ctx,
    impala_udf::StringVal* result) const {
  DCHECK(metadata_ != nullptr);
  auto buffer = CreateStringBuffer(Len() * 2);
  JsonWriter writer(buffer);
  RETURN_IF_ERROR(VariantJsonSerializer::Write(
      *this, *metadata_, &writer));
  *result = impala_udf::StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(buffer.GetString()), buffer.GetSize());
  return Status::OK();
}

Status VariantValue::ToJson(std::ostream* out) const {
  DCHECK(metadata_ != nullptr);
  // Buffer the whole document first so that a decode failure (which can surface after
  // some bytes have already been written to 'writer') does not leak partial JSON.
  auto buffer = CreateStringBuffer(Len() * 2);
  JsonWriter writer(buffer);
  RETURN_IF_ERROR(VariantJsonSerializer::Write(
      *this, *metadata_, &writer));
  out->write(buffer.GetString(), buffer.GetSize());
  return Status::OK();
}

}  // namespace impala
