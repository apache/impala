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

  int pos = 1;
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
  int offsets_len = (dict_size_ + 1) * offset_size_;
  if (pos + offsets_len > len) {
    return Status("Variant metadata too short for offset array");
  }
  pos += offsets_len;

  string_data_ = data + pos;
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
  DCHECK_LT(index, dict_size_);
  uint32_t start = ReadOffset(index);
  uint32_t end = ReadOffset(index + 1);
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

bool VariantValue::GetBoolean() const {
  VariantPhysicalType pt = GetPhysicalType();
  DCHECK(pt == VariantPhysicalType::BOOLEAN_TRUE
      || pt == VariantPhysicalType::BOOLEAN_FALSE);
  return pt == VariantPhysicalType::BOOLEAN_TRUE;
}

int8_t VariantValue::GetInt8() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::INT8);
  return static_cast<int8_t>(data_[1]);
}

int16_t VariantValue::GetInt16() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::INT16);
  return ReadValue<int16_t>();
}

int32_t VariantValue::GetInt32() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::INT32);
  return ReadValue<int32_t>();
}

int64_t VariantValue::GetInt64() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::INT64);
  return ReadValue<int64_t>();
}

float VariantValue::GetFloat() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::FLOAT);
  return ReadValue<float>();
}

double VariantValue::GetDouble() const {
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::DOUBLE);
  return ReadValue<double>();
}

StringValue VariantValue::GetString() const {
  VariantBasicType bt = GetBasicType();
  if (bt == VariantBasicType::SHORT_STRING) {
    int str_len = (data_[0] >> 2) & 0x3F;
    StringValue sv;
    sv.Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 1)),
        str_len);
    return sv;
  }
  DCHECK_EQ(bt, VariantBasicType::PRIMITIVE);
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::STRING);
  uint32_t str_len = ReadUint(data_ + 1, 4);
  StringValue sv;
  sv.Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 5)),
      str_len);
  return sv;
}

StringValue VariantValue::GetBinary() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::PRIMITIVE);
  DCHECK_EQ(GetPhysicalType(), VariantPhysicalType::BINARY);
  uint32_t bin_len = ReadUint(data_ + 1, 4);
  StringValue sv;
  sv.Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(data_ + 5)),
      bin_len);
  return sv;
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

uint32_t VariantValue::ObjectNumFields() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::OBJECT);
  bool is_large = (data_[0] >> 6) & 0x01;
  uint32_t size_bytes = is_large ? 4 : 1;
  return ReadUint(data_ + 1, size_bytes);
}

const uint8_t* VariantValue::ObjectFieldIdsStart() const {
  bool is_large = (data_[0] >> 6) & 0x01;
  uint32_t size_bytes = is_large ? 4 : 1;
  return data_ + 1 + size_bytes;
}

const uint8_t* VariantValue::ObjectOffsetsStart() const {
  uint32_t num_fields = ObjectNumFields();
  uint32_t field_id_size = ObjectFieldIdSize();
  return ObjectFieldIdsStart() + num_fields * field_id_size;
}

const uint8_t* VariantValue::ObjectDataStart() const {
  uint32_t num_fields = ObjectNumFields();
  uint32_t offset_size = ObjectOffsetSize();
  return ObjectOffsetsStart() + (num_fields + 1) * offset_size;
}

uint32_t VariantValue::GetObjectSize() const {
  if (GetBasicType() != VariantBasicType::OBJECT) return 0;
  return ObjectNumFields();
}

bool VariantValue::GetFieldByName(string_view name,
    VariantValue* result) const {
  if (GetBasicType() != VariantBasicType::OBJECT) return false;
  if (metadata_ == nullptr) return false;

  int field_id = metadata_->FindFieldId(name);
  if (field_id < 0) return false;

  uint32_t num_fields = ObjectNumFields();
  uint32_t field_id_size = ObjectFieldIdSize();
  const uint8_t* field_ids = ObjectFieldIdsStart();

  // Search for this field_id in the object's field_id array.
  int field_index = -1;
  for (uint32_t i = 0; i < num_fields; ++i) {
    uint32_t fid = ReadUint(field_ids + i * field_id_size, field_id_size);
    if (fid == field_id) {
      field_index = i;
      break;
    }
  }
  if (field_index < 0) return false;

  return GetFieldByIndex(field_index, result);
}

bool VariantValue::GetFieldByIndex(uint32_t index, VariantValue* result) const {
  if (GetBasicType() != VariantBasicType::OBJECT) return false;
  uint32_t num_fields = ObjectNumFields();
  if (index >= num_fields) return false;

  uint32_t offset_size = ObjectOffsetSize();
  const uint8_t* offsets = ObjectOffsetsStart();
  const uint8_t* data_start = ObjectDataStart();

  uint32_t field_offset =
      ReadUint(offsets + index * offset_size, offset_size);
  uint32_t next_offset =
      ReadUint(offsets + (index + 1) * offset_size, offset_size);
  uint32_t field_len = next_offset - field_offset;

  *result = VariantValue(data_start + field_offset, field_len, metadata_);
  return true;
}

string_view VariantValue::GetFieldNameByIndex(uint32_t index) const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::OBJECT);
  uint32_t field_id_size = ObjectFieldIdSize();
  const uint8_t* field_ids = ObjectFieldIdsStart();
  uint32_t fid =
      ReadUint(field_ids + index * field_id_size, field_id_size);
  return metadata_->GetFieldName(fid);
}

// --- Array accessors ---

uint32_t VariantValue::ArrayOffsetSize() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::ARRAY);
  return ((data_[0] >> 2) & 0x03) + 1;
}

uint32_t VariantValue::ArrayNumElements() const {
  DCHECK_EQ(GetBasicType(), VariantBasicType::ARRAY);
  bool is_large = (data_[0] >> 4) & 0x01;
  uint32_t size_bytes = is_large ? 4 : 1;
  return ReadUint(data_ + 1, size_bytes);
}

const uint8_t* VariantValue::ArrayOffsetsStart() const {
  bool is_large = (data_[0] >> 4) & 0x01;
  uint32_t size_bytes = is_large ? 4 : 1;
  return data_ + 1 + size_bytes;
}

const uint8_t* VariantValue::ArrayDataStart() const {
  uint32_t num_elements = ArrayNumElements();
  uint32_t offset_size = ArrayOffsetSize();
  return ArrayOffsetsStart() + (num_elements + 1) * offset_size;
}

uint32_t VariantValue::GetArraySize() const {
  if (GetBasicType() != VariantBasicType::ARRAY) return 0;
  return ArrayNumElements();
}

bool VariantValue::GetArrayElement(uint32_t index, VariantValue* result) const {
  if (GetBasicType() != VariantBasicType::ARRAY) return false;
  uint32_t num_elements = ArrayNumElements();
  if (index >= num_elements) return false;

  uint32_t offset_size = ArrayOffsetSize();
  const uint8_t* offsets = ArrayOffsetsStart();
  const uint8_t* data_start = ArrayDataStart();

  uint32_t elem_offset =
      ReadUint(offsets + index * offset_size, offset_size);
  uint32_t next_offset =
      ReadUint(offsets + (index + 1) * offset_size, offset_size);
  uint32_t elem_len = next_offset - elem_offset;

  *result = VariantValue(data_start + elem_offset, elem_len, metadata_);
  return true;
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

static Status ValueToJson(const VariantValue& val,
    const VariantMetadata& metadata, JsonWriter* writer,
    impala_udf::FunctionContext* ctx = nullptr) {
  switch (val.GetBasicType()) {
    case VariantBasicType::SHORT_STRING: {
      StringValue sv = val.GetString();
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
        case VariantPhysicalType::INT8:
          writer->Int(val.GetInt8());
          break;
        case VariantPhysicalType::INT16:
          writer->Int(val.GetInt16());
          break;
        case VariantPhysicalType::INT32:
          writer->Int(val.GetInt32());
          break;
        case VariantPhysicalType::INT64:
          writer->Int64(val.GetInt64());
          break;
        case VariantPhysicalType::FLOAT: {
          char buf[24];
          int n = snprintf(buf, sizeof(buf), "%g", val.GetFloat());
          writer->RawValue(buf, n, rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DOUBLE:
          writer->Double(val.GetDouble());
          break;
        case VariantPhysicalType::STRING: {
          StringValue sv = val.GetString();
          writer->String(sv.Ptr(), sv.Len());
          break;
        }
        case VariantPhysicalType::DATE: {
          DateValue dv(static_cast<int64_t>(val.ReadValue<int32_t>()));
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN];
          int n = DateParser::FormatDefault(dv, buf);
          if (LIKELY(n > 0)) {
            DCHECK_LE(n, sizeof(buf));
            writer->String(buf, n);
          } else {
            if (ctx) {
              ctx->AddWarning("Invalid DATE value in VARIANT");
            }
            writer->String("<invalid-date>");
          }
          break;
        }
        case VariantPhysicalType::DECIMAL4: {
          int scale = val.Data()[1];
          int32_t unscaled = val.ReadValue<int32_t>(2);
          string s = Decimal4Value(unscaled).ToString(9, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DECIMAL8: {
          int scale = val.Data()[1];
          int64_t unscaled = val.ReadValue<int64_t>(2);
          string s = Decimal8Value(unscaled).ToString(18, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::DECIMAL16: {
          int scale = val.Data()[1];
          __int128_t unscaled = val.ReadValue<__int128_t>(2);
          string s = Decimal16Value(unscaled).ToString(38, scale);
          writer->RawValue(s.data(), s.size(), rapidjson::kNumberType);
          break;
        }
        case VariantPhysicalType::TIMESTAMPNTZ: {
          int64_t micros = val.ReadValue<int64_t>();
          TimestampValue ts = TimestampValue::UtcFromUnixTimeMicros(micros);
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN];
          int n = TimestampParser::FormatDefault(ts.date(), ts.time(), buf);
          if (LIKELY(n > 0)) {
            DCHECK_LE(n, sizeof(buf));
            writer->String(buf, n);
          } else {
            if (ctx) {
              ctx->AddWarning("Invalid TIMESTAMP value in VARIANT");
            }
            writer->String("<invalid-timestamp>");
          }
          break;
        }
        case VariantPhysicalType::TIMESTAMPNTZ_NANOS: {
          int64_t nanos = val.ReadValue<int64_t>();
          TimestampValue ts =
              TimestampValue::UtcFromUnixTimeLimitedRangeNanos(nanos);
          char buf[SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN];
          int n = TimestampParser::FormatDefault(ts.date(), ts.time(), buf);
          if (LIKELY(n > 0)) {
            DCHECK_LE(n, sizeof(buf));
            writer->String(buf, n);
          } else {
            if (ctx) {
              ctx->AddWarning("Invalid timestamp value in VARIANT");
            }
            writer->String("<invalid-timestamp>");
          }
          break;
        }
        case VariantPhysicalType::BINARY: {
          StringValue sv = val.GetBinary();
          int64_t out_max;
          if (UNLIKELY(!Base64EncodeBufLen(sv.Len(), &out_max))) {
            if (ctx) {
              ctx->AddWarning("Invalid BINARY value in VARIANT");
            }
            writer->String("<invalid-binary>");
            break;
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
      writer->StartObject();
      uint32_t num_fields = val.GetObjectSize();
      for (uint32_t i = 0; i < num_fields; ++i) {
        string_view field_name = val.GetFieldNameByIndex(i);
        writer->Key(field_name.data(), field_name.size());
        VariantValue child;
        if (!val.GetFieldByIndex(i, &child)) {
          return Status("Failed to read object field");
        }
        RETURN_IF_ERROR(ValueToJson(child, metadata, writer));
      }
      writer->EndObject();
      return Status::OK();
    }
    case VariantBasicType::ARRAY: {
      writer->StartArray();
      uint32_t num_elements = val.GetArraySize();
      for (uint32_t i = 0; i < num_elements; ++i) {
        VariantValue elem;
        if (!val.GetArrayElement(i, &elem)) {
          return Status("Failed to read array element");
        }
        RETURN_IF_ERROR(ValueToJson(elem, metadata, writer));
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
  RETURN_IF_ERROR(ValueToJson(*this, *metadata_, &writer));
  json_out->assign(buffer.GetString(), buffer.GetSize());
  return Status::OK();
}

Status VariantValue::ToJson(impala_udf::FunctionContext* ctx,
    impala_udf::StringVal* result) const {
  DCHECK(metadata_ != nullptr);
  auto buffer = CreateStringBuffer(Len() * 2);
  JsonWriter writer(buffer);
  RETURN_IF_ERROR(ValueToJson(*this, *metadata_, &writer, ctx));
  *result = impala_udf::StringVal::CopyFrom(ctx,
      reinterpret_cast<const uint8_t*>(buffer.GetString()), buffer.GetSize());
  return Status::OK();
}

}  // namespace impala
