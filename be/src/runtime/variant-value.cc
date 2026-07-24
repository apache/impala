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
  // Callers (FindFieldId and FieldNameFromLayout) must bound-check the index.
  DCHECK_LT(index, dict_size_);
  uint32_t start = ReadOffset(index);
  uint32_t end = ReadOffset(index + 1);
  // Guaranteed by the offset validation in Init().
  DCHECK_LE(start, end);
  DCHECK_LE(end, string_data_len_);
  return string_view(reinterpret_cast<const char*>(string_data_ + start),
      end - start);
}

int VariantMetadata::FindFieldId(string_view name) const {
  if (is_sorted_) {
    int lo = 0, hi = dict_size_ - 1;
    while (lo <= hi) {
      int mid = (lo + hi) / 2;
      int cmp = name.compare(GetFieldName(mid));
      if (cmp == 0) return mid;
      if (cmp < 0) hi = mid - 1;
      else lo = mid + 1;
    }
  } else {
    for (uint32_t i = 0; i < dict_size_; ++i) {
      if (name == GetFieldName(i)) return i;
    }
  }
  return -1;
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
  out->data_len = static_cast<uint32_t>(len_ - data_start_off);
  return true;
}

bool VariantValue::GetObjectSize(uint32_t* out) const {
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return false;
  *out = layout.num_fields;
  return true;
}

bool VariantValue::GetFieldByName(string_view name, VariantValue* result) const {
  if (metadata_ == nullptr) return false;
  ObjectLayout layout;
  if (!ParseObjectLayout(&layout)) return false;

  int field_id = metadata_->FindFieldId(name);
  if (field_id < 0) return false;

  // Search for this field_id in the object's field_id array.
  uint32_t field_id_size = ObjectFieldIdSize();
  for (uint32_t i = 0; i < layout.num_fields; ++i) {
    uint32_t fid = ReadUint(layout.field_ids + i * field_id_size, field_id_size);
    if (fid == static_cast<uint32_t>(field_id)) return FieldFromLayout(layout, i, result);
  }
  return false;
}

bool VariantValue::FieldFromLayout(const ObjectLayout& layout, uint32_t index,
    VariantValue* result) const {
  if (index >= layout.num_fields) return false;
  uint32_t offset_size = ObjectOffsetSize();
  uint32_t start = ReadUint(layout.offsets + index * offset_size, offset_size);
  uint32_t end = ReadUint(layout.offsets + (index + 1) * offset_size, offset_size);
  if (end < start || end > layout.data_len) return false;
  *result = VariantValue(layout.data + start, end - start, metadata_);
  return true;
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
  out->data_len = static_cast<uint32_t>(len_ - data_start_off);
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
  uint32_t end = ReadUint(layout.offsets + (index + 1) * offset_size, offset_size);
  if (end < start || end > layout.data_len) return false;
  *result = VariantValue(layout.data + start, end - start, metadata_);
  return true;
}

bool VariantValue::GetArrayElement(uint32_t index, VariantValue* result) const {
  ArrayLayout layout;
  if (!ParseArrayLayout(&layout)) return false;
  return ElementFromLayout(layout, index, result);
}

// --- Path navigation ---

bool VariantValue::NavigatePath(const string& path,
    VariantValue* result) const {
  *result = *this;
  if (path == "$") return true;

  const char* p = path.data();
  const char* end = p + path.size();

  // Path must start with '$'.
  if (p >= end || *p != '$') return false;
  ++p;

  // '$' alone is handled above; after '$' we need '.' or '['.
  if (p >= end) return false;
  if (*p != '.' && *p != '[') return false;
  if (*p == '.') ++p;

  // Must have at least one segment after the prefix.
  if (p >= end) return false;

  while (p < end) {
    if (*p == '[') {
      ++p;
      // Require at least one digit.
      const char* digits_start = p;
      int index = 0;
      while (p < end && *p >= '0' && *p <= '9') {
        index = index * 10 + (*p - '0');
        ++p;
      }
      if (p == digits_start) return false;
      if (p >= end || *p != ']') return false;
      ++p;  // skip ']'
      if (!result->GetArrayElement(index, result)) return false;
      if (p < end && *p == '.') ++p;
    } else {
      const char* seg_start = p;
      while (p < end && *p != '.' && *p != '[') ++p;
      int seg_len = p - seg_start;
      if (seg_len == 0) return false;
      if (!result->GetFieldByName(string_view(seg_start, seg_len), result)) {
        return false;
      }
      if (p < end && *p == '.') ++p;
    }
  }
  return true;
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
          return Status("Failed to read object field");
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
          return Status("Failed to read array element");
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
