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

#include "util/min-max-filter.h"

#include <sstream>
#include <unordered_map>

#include "common/object-pool.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.inline.h"

using std::numeric_limits;
using std::stringstream;

namespace impala {

static std::unordered_map<int, string> MIN_MAX_FILTER_LLVM_CLASS_NAMES = {
    {PrimitiveType::TYPE_BOOLEAN, BoolMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_TINYINT, TinyIntMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_SMALLINT, SmallIntMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_INT, IntMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_BIGINT, BigIntMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_FLOAT, FloatMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_DOUBLE, DoubleMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_STRING, StringMinMaxFilter::LLVM_CLASS_NAME},
    {PrimitiveType::TYPE_TIMESTAMP, TimestampMinMaxFilter::LLVM_CLASS_NAME}};

static std::unordered_map<int, IRFunction::Type> MIN_MAX_FILTER_IR_FUNCTION_TYPES = {
    {PrimitiveType::TYPE_BOOLEAN, IRFunction::BOOL_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_TINYINT, IRFunction::TINYINT_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_SMALLINT, IRFunction::SMALLINT_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_INT, IRFunction::INT_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_BIGINT, IRFunction::BIGINT_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_FLOAT, IRFunction::FLOAT_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_DOUBLE, IRFunction::DOUBLE_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_STRING, IRFunction::STRING_MIN_MAX_FILTER_INSERT},
    {PrimitiveType::TYPE_TIMESTAMP, IRFunction::TIMESTAMP_MIN_MAX_FILTER_INSERT}};

string MinMaxFilter::GetLlvmClassName(PrimitiveType type) {
  return MIN_MAX_FILTER_LLVM_CLASS_NAMES[type];
}

IRFunction::Type MinMaxFilter::GetInsertIRFunctionType(PrimitiveType type) {
  return MIN_MAX_FILTER_IR_FUNCTION_TYPES[type];
}

#define NUMERIC_MIN_MAX_FILTER_FUNCS(NAME, TYPE, THRIFT_TYPE, PRIMITIVE_TYPE)  \
  const char* NAME##MinMaxFilter::LLVM_CLASS_NAME =                            \
      "class.impala::" #NAME "MinMaxFilter";                                   \
  NAME##MinMaxFilter::NAME##MinMaxFilter(const TMinMaxFilter& thrift) {        \
    DCHECK(!thrift.always_true);                                               \
    if (thrift.always_false) {                                                 \
      min_ = numeric_limits<TYPE>::max();                                      \
      max_ = numeric_limits<TYPE>::lowest();                                   \
    } else {                                                                   \
      DCHECK(thrift.__isset.min);                                              \
      DCHECK(thrift.__isset.max);                                              \
      DCHECK(thrift.min.__isset.THRIFT_TYPE##_val);                            \
      DCHECK(thrift.max.__isset.THRIFT_TYPE##_val);                            \
      min_ = thrift.min.THRIFT_TYPE##_val;                                     \
      max_ = thrift.max.THRIFT_TYPE##_val;                                     \
    }                                                                          \
  }                                                                            \
  PrimitiveType NAME##MinMaxFilter::type() {                                   \
    return PrimitiveType::TYPE_##PRIMITIVE_TYPE;                               \
  }                                                                            \
  void NAME##MinMaxFilter::ToThrift(TMinMaxFilter* thrift) const {             \
    if (!AlwaysFalse()) {                                                      \
      thrift->min.__set_##THRIFT_TYPE##_val(min_);                             \
      thrift->__isset.min = true;                                              \
      thrift->max.__set_##THRIFT_TYPE##_val(max_);                             \
      thrift->__isset.max = true;                                              \
    }                                                                          \
    thrift->__set_always_false(AlwaysFalse());                                 \
    thrift->__set_always_true(false);                                          \
  }                                                                            \
  string NAME##MinMaxFilter::DebugString() const {                             \
    stringstream out;                                                          \
    out << #NAME << "MinMaxFilter(min=" << min_ << ", max=" << max_            \
        << ", always_false=" << (AlwaysFalse() ? "true" : "false") << ")";     \
    return out.str();                                                          \
  }                                                                            \
  void NAME##MinMaxFilter::Or(const TMinMaxFilter& in, TMinMaxFilter* out) {   \
    if (out->always_false) {                                                   \
      out->min.__set_##THRIFT_TYPE##_val(in.min.THRIFT_TYPE##_val);            \
      out->__isset.min = true;                                                 \
      out->max.__set_##THRIFT_TYPE##_val(in.max.THRIFT_TYPE##_val);            \
      out->__isset.max = true;                                                 \
      out->__set_always_false(false);                                          \
    } else {                                                                   \
      out->min.__set_##THRIFT_TYPE##_val(                                      \
          std::min(in.min.THRIFT_TYPE##_val, out->min.THRIFT_TYPE##_val));     \
      out->max.__set_##THRIFT_TYPE##_val(                                      \
          std::max(in.max.THRIFT_TYPE##_val, out->max.THRIFT_TYPE##_val));     \
    }                                                                          \
  }                                                                            \
  void NAME##MinMaxFilter::Copy(const TMinMaxFilter& in, TMinMaxFilter* out) { \
    out->min.__set_##THRIFT_TYPE##_val(in.min.THRIFT_TYPE##_val);              \
    out->__isset.min = true;                                                   \
    out->max.__set_##THRIFT_TYPE##_val(in.max.THRIFT_TYPE##_val);              \
    out->__isset.max = true;                                                   \
  }

NUMERIC_MIN_MAX_FILTER_FUNCS(Bool, bool, bool, BOOLEAN);
NUMERIC_MIN_MAX_FILTER_FUNCS(TinyInt, int8_t, byte, TINYINT);
NUMERIC_MIN_MAX_FILTER_FUNCS(SmallInt, int16_t, short, SMALLINT);
NUMERIC_MIN_MAX_FILTER_FUNCS(Int, int32_t, int, INT);
NUMERIC_MIN_MAX_FILTER_FUNCS(BigInt, int64_t, long, BIGINT);
NUMERIC_MIN_MAX_FILTER_FUNCS(Float, float, double, FLOAT);
NUMERIC_MIN_MAX_FILTER_FUNCS(Double, double, double, DOUBLE);

int64_t GetIntTypeMax(const ColumnType& type) {
  switch (type.type) {
    case TYPE_TINYINT:
      return numeric_limits<int8_t>::max();
    case TYPE_SMALLINT:
      return numeric_limits<int16_t>::max();
    case TYPE_INT:
      return numeric_limits<int32_t>::max();
    case TYPE_BIGINT:
      return numeric_limits<int64_t>::max();
    default:
      DCHECK(false) << "Not an int type: " << type;
  }
  return -1;
}

int64_t GetIntTypeMin(const ColumnType& type) {
  switch (type.type) {
    case TYPE_TINYINT:
      return numeric_limits<int8_t>::lowest();
    case TYPE_SMALLINT:
      return numeric_limits<int16_t>::lowest();
    case TYPE_INT:
      return numeric_limits<int32_t>::lowest();
    case TYPE_BIGINT:
      return numeric_limits<int64_t>::lowest();
    default:
      DCHECK(false) << "Not an int type: " << type;
  }
  return -1;
}

#define NUMERIC_MIN_MAX_FILTER_CAST(NAME)                           \
  bool NAME##MinMaxFilter::GetCastIntMinMax(                        \
      const ColumnType& type, int64_t* out_min, int64_t* out_max) { \
    int64_t type_min = GetIntTypeMin(type);                         \
    int64_t type_max = GetIntTypeMax(type);                         \
    if (min_ < type_min) {                                          \
      *out_min = type_min;                                          \
    } else if (min_ > type_max) {                                   \
      return false;                                                 \
    } else {                                                        \
      *out_min = min_;                                              \
    }                                                               \
    if (max_ > type_max) {                                          \
      *out_max = type_max;                                          \
    } else if (max_ < type_min) {                                   \
      return false;                                                 \
    } else {                                                        \
      *out_max = max_;                                              \
    }                                                               \
    return true;                                                    \
  }

NUMERIC_MIN_MAX_FILTER_CAST(TinyInt);
NUMERIC_MIN_MAX_FILTER_CAST(SmallInt);
NUMERIC_MIN_MAX_FILTER_CAST(Int);
NUMERIC_MIN_MAX_FILTER_CAST(BigInt);

#define NUMERIC_MIN_MAX_FILTER_NO_CAST(NAME)                                           \
  bool NAME##MinMaxFilter::GetCastIntMinMax(                                           \
      const ColumnType& type, int64_t* out_min, int64_t* out_max) {                    \
    DCHECK(false) << "Casting min-max filters of type " << #NAME << " not supported."; \
    return true;                                                                       \
  }

NUMERIC_MIN_MAX_FILTER_NO_CAST(Bool);
NUMERIC_MIN_MAX_FILTER_NO_CAST(Float);
NUMERIC_MIN_MAX_FILTER_NO_CAST(Double);

// STRING
const char* StringMinMaxFilter::LLVM_CLASS_NAME = "class.impala::StringMinMaxFilter";
const int StringMinMaxFilter::MAX_BOUND_LENGTH = 1024;

StringMinMaxFilter::StringMinMaxFilter(const TMinMaxFilter& thrift, MemPool* mem_pool)
  : min_buffer_(mem_pool), max_buffer_(mem_pool) {
  always_false_ = thrift.always_false;
  always_true_ = thrift.always_true;
  if (!always_true_ && !always_false_) {
    DCHECK(thrift.__isset.min);
    DCHECK(thrift.__isset.max);
    DCHECK(thrift.min.__isset.string_val);
    DCHECK(thrift.max.__isset.string_val);
    min_ = StringValue(thrift.min.string_val);
    max_ = StringValue(thrift.max.string_val);
    CopyToBuffer(&min_buffer_, &min_, min_.len);
    CopyToBuffer(&max_buffer_, &max_, max_.len);
  }
}

PrimitiveType StringMinMaxFilter::type() {
  return PrimitiveType::TYPE_STRING;
}

void StringMinMaxFilter::MaterializeValues() {
  if (always_true_ || always_false_) return;
  if (min_buffer_.IsEmpty()) {
    if (min_.len > MAX_BOUND_LENGTH) {
      // Truncating 'value' gives a valid min bound as the result will be <= 'value'.
      CopyToBuffer(&min_buffer_, &min_, MAX_BOUND_LENGTH);
    } else {
      CopyToBuffer(&min_buffer_, &min_, min_.len);
    }
  }
  if (max_buffer_.IsEmpty()) {
    if (max_.len > MAX_BOUND_LENGTH) {
      CopyToBuffer(&max_buffer_, &max_, MAX_BOUND_LENGTH);
      if (always_true_) return;
      // After truncating 'value', to still have a valid max bound we add 1 to one char in
      // the string, so that the result will be > 'value'. If the entire string is already
      // the max char, then disable this filter by making it always_true.
      int i = MAX_BOUND_LENGTH - 1;
      while (i >= 0 && static_cast<int32_t>(max_buffer_.buffer()[i]) == -1) {
        max_buffer_.buffer()[i] = max_buffer_.buffer()[i] + 1;
        --i;
      }
      if (i == -1) {
        SetAlwaysTrue();
        return;
      }
      max_buffer_.buffer()[i] = max_buffer_.buffer()[i] + 1;
    } else {
      CopyToBuffer(&max_buffer_, &max_, max_.len);
    }
  }
}

void StringMinMaxFilter::ToThrift(TMinMaxFilter* thrift) const {
  if (!always_true_ && !always_false_) {
    thrift->min.string_val.assign(static_cast<char*>(min_.ptr), min_.len);
    thrift->min.__isset.string_val = true;
    thrift->__isset.min = true;
    thrift->max.string_val.assign(static_cast<char*>(max_.ptr), max_.len);
    thrift->max.__isset.string_val = true;
    thrift->__isset.max = true;
  }
  thrift->__set_always_false(always_false_);
  thrift->__set_always_true(always_true_);
}

string StringMinMaxFilter::DebugString() const {
  stringstream out;
  out << "StringMinMaxFilter(min=" << min_ << ", max=" << max_
      << ", always_false=" << (always_false_ ? "true" : "false")
      << ", always_true=" << (always_true_ ? "true" : "false") << ")";
  return out.str();
}

void StringMinMaxFilter::Or(const TMinMaxFilter& in, TMinMaxFilter* out) {
  if (out->always_false) {
    out->min.__set_string_val(in.min.string_val);
    out->__isset.min = true;
    out->max.__set_string_val(in.max.string_val);
    out->__isset.max = true;
    out->__set_always_false(false);
  } else {
    StringValue in_min_val = StringValue(in.min.string_val);
    StringValue out_min_val = StringValue(out->min.string_val);
    if (in_min_val < out_min_val) out->min.__set_string_val(in.min.string_val);
    StringValue in_max_val = StringValue(in.max.string_val);
    StringValue out_max_val = StringValue(out->max.string_val);
    if (in_max_val > out_max_val) out->max.__set_string_val(in.max.string_val);
  }
}

void StringMinMaxFilter::Copy(const TMinMaxFilter& in, TMinMaxFilter* out) {
  out->min.__set_string_val(in.min.string_val);
  out->__isset.min = true;
  out->max.__set_string_val(in.max.string_val);
  out->__isset.max = true;
}

void StringMinMaxFilter::CopyToBuffer(
    StringBuffer* buffer, StringValue* value, int64_t len) {
  if (value->ptr == buffer->buffer()) return;
  buffer->Clear();
  if (!buffer->Append(value->ptr, len).ok()) {
    // If Append() fails, for example because we're out of memory, disable the filter.
    SetAlwaysTrue();
    return;
  }
  value->ptr = buffer->buffer();
  value->len = len;
}

void StringMinMaxFilter::SetAlwaysTrue() {
  always_true_ = true;
  max_buffer_.Clear();
  min_buffer_.Clear();
  min_.ptr = nullptr;
  min_.len = 0;
  max_.ptr = nullptr;
  max_.len = 0;
}

// TIMESTAMP
const char* TimestampMinMaxFilter::LLVM_CLASS_NAME =
    "class.impala::TimestampMinMaxFilter";

TimestampMinMaxFilter::TimestampMinMaxFilter(const TMinMaxFilter& thrift) {
  always_false_ = thrift.always_false;
  if (!always_false_) {
    DCHECK(thrift.min.__isset.timestamp_val);
    DCHECK(thrift.max.__isset.timestamp_val);
    min_ = TimestampValue::FromTColumnValue(thrift.min);
    max_ = TimestampValue::FromTColumnValue(thrift.max);
  }
}

PrimitiveType TimestampMinMaxFilter::type() {
  return PrimitiveType::TYPE_TIMESTAMP;
}

void TimestampMinMaxFilter::ToThrift(TMinMaxFilter* thrift) const {
  if (!always_false_) {
    min_.ToTColumnValue(&thrift->min);
    thrift->__isset.min = true;
    max_.ToTColumnValue(&thrift->max);
    thrift->__isset.max = true;
  }
  thrift->__set_always_false(always_false_);
  thrift->__set_always_true(false);
}

string TimestampMinMaxFilter::DebugString() const {
  stringstream out;
  out << "TimestampMinMaxFilter(min=" << min_ << ", max=" << max_
      << " always_false=" << (always_false_ ? "true" : "false") << ")";
  return out.str();
}

void TimestampMinMaxFilter::Or(const TMinMaxFilter& in, TMinMaxFilter* out) {
  if (out->always_false) {
    out->min.__set_timestamp_val(in.min.timestamp_val);
    out->__isset.min = true;
    out->max.__set_timestamp_val(in.max.timestamp_val);
    out->__isset.max = true;
    out->__set_always_false(false);
  } else {
    TimestampValue in_min_val = TimestampValue::FromTColumnValue(in.min);
    TimestampValue out_min_val = TimestampValue::FromTColumnValue(out->min);
    if (in_min_val < out_min_val) out->min.__set_timestamp_val(in.min.timestamp_val);
    TimestampValue in_max_val = TimestampValue::FromTColumnValue(in.max);
    TimestampValue out_max_val = TimestampValue::FromTColumnValue(out->max);
    if (in_max_val > out_max_val) out->max.__set_timestamp_val(in.max.timestamp_val);
  }
}

void TimestampMinMaxFilter::Copy(const TMinMaxFilter& in, TMinMaxFilter* out) {
  out->min.__set_timestamp_val(in.min.timestamp_val);
  out->__isset.min = true;
  out->max.__set_timestamp_val(in.max.timestamp_val);
  out->__isset.max = true;
}

// MinMaxFilter
bool MinMaxFilter::GetCastIntMinMax(
    const ColumnType& type, int64_t* out_min, int64_t* out_max) {
  DCHECK(false) << "Casting min-max filters of type " << this->type()
      << " not supported.";
  return true;
}

MinMaxFilter* MinMaxFilter::Create(ColumnType type, ObjectPool* pool, MemPool* mem_pool) {
  switch (type.type) {
    case PrimitiveType::TYPE_BOOLEAN:
      return pool->Add(new BoolMinMaxFilter());
    case PrimitiveType::TYPE_TINYINT:
      return pool->Add(new TinyIntMinMaxFilter());
    case PrimitiveType::TYPE_SMALLINT:
      return pool->Add(new SmallIntMinMaxFilter());
    case PrimitiveType::TYPE_INT:
      return pool->Add(new IntMinMaxFilter());
    case PrimitiveType::TYPE_BIGINT:
      return pool->Add(new BigIntMinMaxFilter());
    case PrimitiveType::TYPE_FLOAT:
      return pool->Add(new FloatMinMaxFilter());
    case PrimitiveType::TYPE_DOUBLE:
      return pool->Add(new DoubleMinMaxFilter());
    case PrimitiveType::TYPE_STRING:
      return pool->Add(new StringMinMaxFilter(mem_pool));
    case PrimitiveType::TYPE_TIMESTAMP:
      return pool->Add(new TimestampMinMaxFilter());
    default:
      DCHECK(false) << "Unsupported MinMaxFilter type: " << type;
  }
  return nullptr;
}

MinMaxFilter* MinMaxFilter::Create(
    const TMinMaxFilter& thrift, ColumnType type, ObjectPool* pool, MemPool* mem_pool) {
  switch (type.type) {
    case PrimitiveType::TYPE_BOOLEAN:
      return pool->Add(new BoolMinMaxFilter(thrift));
    case PrimitiveType::TYPE_TINYINT:
      return pool->Add(new TinyIntMinMaxFilter(thrift));
    case PrimitiveType::TYPE_SMALLINT:
      return pool->Add(new SmallIntMinMaxFilter(thrift));
    case PrimitiveType::TYPE_INT:
      return pool->Add(new IntMinMaxFilter(thrift));
    case PrimitiveType::TYPE_BIGINT:
      return pool->Add(new BigIntMinMaxFilter(thrift));
    case PrimitiveType::TYPE_FLOAT:
      return pool->Add(new FloatMinMaxFilter(thrift));
    case PrimitiveType::TYPE_DOUBLE:
      return pool->Add(new DoubleMinMaxFilter(thrift));
    case PrimitiveType::TYPE_STRING:
      return pool->Add(new StringMinMaxFilter(thrift, mem_pool));
    case PrimitiveType::TYPE_TIMESTAMP:
      return pool->Add(new TimestampMinMaxFilter(thrift));
    default:
      DCHECK(false) << "Unsupported MinMaxFilter type: " << type;
  }
  return nullptr;
}

void MinMaxFilter::Or(const TMinMaxFilter& in, TMinMaxFilter* out) {
  if (in.always_false || out->always_true) return;
  if (in.always_true) {
    out->__set_always_true(true);
    return;
  }
  if (in.min.__isset.bool_val) {
    DCHECK(out->min.__isset.bool_val);
    BoolMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.byte_val) {
    DCHECK(out->min.__isset.byte_val);
    TinyIntMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.short_val) {
    DCHECK(out->min.__isset.short_val);
    SmallIntMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.int_val) {
    DCHECK(out->min.__isset.int_val);
    IntMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.long_val) {
    DCHECK(out->min.__isset.long_val);
    BigIntMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.double_val) {
    // Handles FloatMinMaxFilter also as TColumnValue doesn't have a float type.
    DCHECK(out->min.__isset.double_val);
    DoubleMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.string_val) {
    DCHECK(out->min.__isset.string_val);
    StringMinMaxFilter::Or(in, out);
    return;
  } else if (in.min.__isset.timestamp_val) {
    DCHECK(out->min.__isset.timestamp_val);
    TimestampMinMaxFilter::Or(in, out);
    return;
  }
  DCHECK(false) << "Unsupported MinMaxFilter type.";
}

void MinMaxFilter::Copy(const TMinMaxFilter& in, TMinMaxFilter* out) {
  out->__set_always_false(in.always_false);
  out->__set_always_true(in.always_true);
  if (in.always_false || in.always_true) return;
  if (in.min.__isset.bool_val) {
    DCHECK(!out->min.__isset.bool_val);
    BoolMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.byte_val) {
    DCHECK(!out->min.__isset.byte_val);
    TinyIntMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.short_val) {
    DCHECK(!out->min.__isset.short_val);
    SmallIntMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.int_val) {
    DCHECK(!out->min.__isset.int_val);
    IntMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.long_val) {
    // Handles TimestampMinMaxFilter also as TColumnValue doesn't have a timestamp type.
    DCHECK(!out->min.__isset.long_val);
    BigIntMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.double_val) {
    // Handles FloatMinMaxFilter also as TColumnValue doesn't have a float type.
    DCHECK(!out->min.__isset.double_val);
    DoubleMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.string_val) {
    DCHECK(!out->min.__isset.string_val);
    StringMinMaxFilter::Copy(in, out);
    return;
  } else if (in.min.__isset.timestamp_val) {
    DCHECK(!out->min.__isset.timestamp_val);
    TimestampMinMaxFilter::Copy(in, out);
    return;
  }
  DCHECK(false) << "Unsupported MinMaxFilter type.";
}

} // namespace impala
